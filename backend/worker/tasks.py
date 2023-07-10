import datetime

from celery import group
from celery.signals import worker_init
from worker.celery_app import celery_app

logger = get_logger(__name__)


@celery_app.task(
    name="tasks.price_curve_collection_initiator",
    bind=True,
    default_retry_delay=15 * 60,
    base=DatabaseTask
)
def price_curve_collection_initiator(self, source_slug: str):
    try:
        session = self.session
        products = product_manager.get_by_source_slug(db_session=session, slug=source_slug)
        job = group(
            [
                price_curve_collector.s(source_slug, product.id, product.symbol).set(
                    countdown=product.collection_delay * 60
                )
                for product in products
            ]
        )

        return job.apply_async()
    except Exception as exc:
        logger.exception(exc)
        sentry_sdk.capture_exception(exc)
        self.retry(exc=exc, max_retries=5)


@celery_app.task(
    name="tasks.price_curve_collector", bind=True, default_retry_delay=30 * 60,
    base=DatabaseTask
)
def price_curve_collector(self, source_slug: str, product_id: str, symbol: str):
    session = self.session
    try:
        logger.info(
            "Collection Price Curve",
            extra={"symbol": symbol, "source_slug": source_slug},
        )

        start_date = None
        today = arrow.now("US/Central")

        previous_contract = None
        [nearest_contract] = get_quotes([symbol])

        if arrow.get(nearest_contract.expiration_date).date() == today.date():
            previous_contract = nearest_contract
        else:
            active_contract = nearest_contract
            last_12m_quotes = get_historical_prices(f"{symbol}*1", PricingFrequency.MONTHLY.value,
                                                    today.shift(months=-12).datetime,
                                                    today.datetime)

            for quote in reversed(last_12m_quotes):
                if quote.symbol != active_contract.symbol:
                    previous_contract = quote
                    break

        if previous_contract:
            previous_contract_timestamp = get_timestamp_for_symbol(previous_contract.symbol)
            start_date = previous_contract_timestamp.shift(months=1)

        last_price_curve = price_curve_manager.get_by_latest_by_product_id(
            db_session=session, product_id=product_id
        )
        price_curves = get_price_curves(
            symbol=symbol,
            start_date=start_date,
            num_months=24,
            source_slug=SourceSlug(source_slug),
            logger=logger,
        )

        if not price_curves:
            logger.info(
                "No Price Curves", extra={"symbol": symbol, "source_slug": source_slug}
            )
            raise Exception("No Price Curves")

        averages = calculate_price_curve_averages(curves=price_curves)

        if not last_price_curve:
            price_curve_manager.create(
                db_session=session,
                product_id=product_id,
                prices=price_curves,
                averages=averages,
                pct_diffs={x: 0.0 for x in range(1, 25)},
                net_diffs={x: 0.0 for x in range(1, 25)},
                commit=True,
            )
        else:
            net_diffs = {}
            pct_diffs = {}
            for idx in range(1, 25):
                idx_str = str(idx)
                previous = last_price_curve.averages[idx_str]
                current = averages[idx]
                net_diff = current - previous
                pct_diff = net_diff / current
                net_diffs[idx] = net_diff
                pct_diffs[idx] = pct_diff

            curve = price_curve_manager.create(
                db_session=session,
                product_id=product_id,
                prices=price_curves,
                averages=averages,
                pct_diffs=pct_diffs,
                net_diffs=net_diffs,
                commit=True,
            )

            return str(curve.id)
    except Exception as exc:
        logger.exception(exc)
        sentry_sdk.capture_exception(exc)
        self.retry(exc=exc, max_retries=5)


@celery_app.task(name="tasks.alerts_monitor", bind=True, default_retry_delay=15 * 60, base=DatabaseTask)
def alerts_monitor(self, source_slug: str):
    session = self.session
    try:

        alerts = alert_manager.get_by_source_slug(
            db_session=session, source_slug=SourceSlug(source_slug)
        )
        job = group(
            [
                alert_processor.s(alert.id).set(countdown=i + 5)
                for i, alert in enumerate(alerts)
            ]
        )
        return job.apply_async()

    except Exception as exc:
        logger.exception(exc)
        sentry_sdk.capture_exception(exc)
        self.retry(exc=exc, max_retries=5)


@celery_app.task(name="tasks.alert_processor", bind=True, default_retry_delay=30 * 60, base=DatabaseTask)
def alert_processor(self, alert_id: str):
    session = self.session
    try:
        alert = alert_manager.get(db_session=session, id=alert_id)
        quote_field_to_use = "settlement"
        if alert.product.source.collector == Collector.BARCHART:
            if alert.alert_type in {AlertType.CONTRACT_NEAREST_DELIVERY, AlertType.BOARD_PRODUCT_NEAREST_DELIVERY}:
                symbol = alert.product.symbol + "^f"
            elif alert.alert_type in {AlertType.BOARD_PRODUCT_SPECIFIC_MONTH, AlertType.CONTRACT_FORECASTED_PRICE}:
                symbol = alert.specific_month_ticker
            elif alert.alert_type == AlertType.BOARD_TOTAL_COST:
                #  Take only transactions, SUM(for each one calculate (price + premium) * quantity)
                # If no price and the transaction in the future, take the latest price curve of the delivery month of the transaction date. if not possible to backfill, set the price to 0.
                board = get_board(alert.board_id)
                currency = alert.product.currency
                currency_divisor = 100 if currency == 'usc' else 1
                transactions = board.revision.transactions
                total_transactions_cost = 0
                for transaction in transactions:
                    if not transaction.premium:
                        transaction.premium = 0.0

                    if transaction.price:
                        total_transactions_cost += ((
                                                            transaction.price + transaction.premium) / currency_divisor) * transaction.quantity
                    else:
                        contract_month = transaction.datetime.strftime("%m/%Y")
                        price_curve = price_curve_manager.get_latest_by_product_id_and_contract_month(
                            db_session=session,
                            product_id=alert.product_id,
                            contract_month=contract_month)
                        backfilled_prices = backfill_price_curves(price_curve.prices)
                        if price_data := backfilled_prices.get(contract_month):
                            total_transactions_cost += ((price_data.get('calculated_settlement', price_data.get(
                                'settlement')) + transaction.premium) / currency_divisor) * transaction.quantity

                if alert.comparison_operator == AlertComparisonOperator.GREATER_THAN_EQUAL:
                    if total_transactions_cost >= alert.threshold_value:
                        alert_triggered(
                            db_session=session,
                            alert=alert,
                            event_metadata={
                                "trigger_value": total_transactions_cost,
                                "threshold_value": alert.threshold_value,
                                "creation_metadata": alert.creation_metadata,
                                "comparison_operator": alert.comparison_operator,
                            },
                        )
                elif alert.comparison_operator == AlertComparisonOperator.LESS_THAN_EQUAL:
                    if total_transactions_cost <= alert.threshold_value:
                        alert_triggered(
                            db_session=session,
                            alert=alert,
                            event_metadata={
                                "trigger_value": total_transactions_cost,
                                "threshold_value": alert.threshold_value,
                                "creation_metadata": alert.creation_metadata,
                                "comparison_operator": alert.comparison_operator,
                            },
                        )
                return
            else:
                raise Exception(f"Unsupported Alert Type: {alert.alert_type}")
            quotes = get_quotes_for_alerts(
                symbol=symbol[:-3] + "^f" if alert.specific_month_ticker else symbol,
                source_slug=alert.product.source.slug
            )

        elif alert.product.source.collector == Collector.OPIS:
            quotes = [
                quote_manager.get_by_latest_settlement_by_product_id(
                    db_session=session, product_id=alert.product.id
                )
            ]
        else:
            raise Exception(f"Alerts for {alert.product.source.collector} are not supported")

        if alert.metric == AlertMetric.EOD_PRICE:
            first_quote = None
            if len(quotes) > 0:
                first_quote = quotes[0]

            if alert.alert_type in {AlertType.BOARD_PRODUCT_SPECIFIC_MONTH, AlertType.CONTRACT_FORECASTED_PRICE}:
                month_tickers = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                month = month_tickers.index(alert.specific_month_ticker[-3]) + 1
                year = int(alert.specific_month_ticker[-2:]) + 2000
                contract_month = f"{str(month).zfill(2)}/{year}"

                if first_quote and (((alert.product.source.collector == Collector.BARCHART and ((
                                                                                                        year >= arrow.get(
                                                                                                    first_quote.expiration_date).year) or (
                                                                                                        (
                                                                                                                year == arrow.get(
                                                                                                            first_quote.expiration_date).year) and month > arrow.get(
                                                                                                    first_quote.expiration_date).month)))) or
                                    ((alert.product.source.collector == Collector.BARCHART and ((
                                                                                                        year <= arrow.get(
                                                                                                    first_quote.expiration_date).year) or (
                                                                                                        (
                                                                                                                year == arrow.get(
                                                                                                            first_quote.expiration_date).year) and month < arrow.get(
                                                                                                    first_quote.expiration_date).month)))) or (
                                            alert.product.source.collector == Collector.OPIS and
                                            (year >= arrow.get(first_quote.timestamp).year) or (
                                                    year == arrow.get(first_quote.timestamp).year and month > arrow.get(
                                                first_quote.timestamp).month))):

                    price_curve = price_curve_manager.get_latest_by_product_id_and_contract_month(db_session=session,
                                                                                                  product_id=alert.product_id,
                                                                                                  contract_month=contract_month)
                    if not price_curve:
                        quotes = get_quotes_for_alerts(alert.specific_month_ticker, alert.product.source.slug)
                        if len(quotes) > 0:
                            first_quote = quotes[0].dict()
                        else:
                            print(f"No Quote: {first_quote.dict()}")
                            # TODO: Handle no price
                            return
                    else:
                        backfilled_prices = backfill_price_curves(price_curve.prices)
                        first_quote = backfilled_prices.get(contract_month)

                    if first_quote:
                        if isinstance(first_quote['expiration_date'], str):
                            first_quote['expiration_date'] = datetime.datetime.strptime(first_quote['expiration_date'],
                                                                                        "%Y-%m-%d")
                        first_quote = AlertQuote(**first_quote)

                    if first_quote and ((alert.product.source.collector == Collector.BARCHART)
                                        and (arrow.get(first_quote.timestamp) >= arrow.get(
                                first_quote.expiration_date))):
                        alert.active = False
                        session.add(alert)
                        session.commit()

                    if not first_quote.settlement:
                        print(f"No Settlement: {first_quote.dict()}")
                        # TODO: Handle no price
                        return
                elif alert.product.source.collector == Collector.OPIS and month < arrow.get(
                        first_quote.timestamp).month:
                    first_quote = quote_manager.get_by_product_id_specific_month(
                        db_session=session, product_id=alert.product_id,
                        dt=datetime.date(year=year, month=month, day=1)
                    )


            elif alert.alert_type == AlertType.BOARD_TOTAL_COST:
                board = get_board(alert.board_id)
                transactions = board.revision.transactions
                currency = alert.product.currency
                currency_divisor = 100 if currency == 'usc' else 1
                total_transactions_cost = 0
                for i in range(len(transactions)):
                    transaction = transactions[i]
                    if not transaction.premium:
                        transaction.premium = 0.0

                    if transaction.price:
                        total_transactions_cost += ((
                                                            transaction.price + transaction.premium) / currency_divisor) * transaction.quantity
                    else:
                        contract_month = transaction.datetime.strftime("%m/%Y")
                        price_curve = price_curve_manager.get_latest_by_product_id_and_contract_month(
                            db_session=session,
                            product_id=alert.product_id,
                            contract_month=contract_month)
                        backfilled_prices = backfill_price_curves(price_curve.prices)
                        price_data = backfilled_prices.get(contract_month)
                        if price_data:
                            total_transactions_cost += ((price_data.get(
                                'settlement') + transaction.premium) / currency_divisor) * transaction.quantity
                        else:
                            print(f"No price data for: {contract_month}")
                            # TODO: Handle no price

                if alert.comparison_operator == AlertComparisonOperator.GREATER_THAN_EQUAL:
                    if total_transactions_cost >= alert.threshold_value:
                        alert_triggered(
                            db_session=session,
                            alert=alert,
                            event_metadata={
                                "trigger_value": total_transactions_cost,
                                "threshold_value": alert.threshold_value,
                                "creation_metadata": alert.creation_metadata,
                                "comparison_operator": alert.comparison_operator,
                            },
                        )
                elif alert.comparison_operator == AlertComparisonOperator.LESS_THAN_EQUAL:
                    if total_transactions_cost <= alert.threshold_value:
                        alert_triggered(
                            db_session=session,
                            alert=alert,
                            event_metadata={
                                "trigger_value": total_transactions_cost,
                                "threshold_value": alert.threshold_value,
                                "creation_metadata": alert.creation_metadata,
                                "comparison_operator": alert.comparison_operator,
                            },
                        )

                return

        if alert.comparison_operator == AlertComparisonOperator.GREATER_THAN_EQUAL:
            if getattr(first_quote, "calculated_settlement" if (hasattr(first_quote,
                                                                        "calculated_settlement") and first_quote.calculated_settlement is not None) else quote_field_to_use) >= alert.threshold_value:
                alert_triggered(
                    db_session=session,
                    alert=alert,
                    event_metadata={
                        "trigger_value": getattr(first_quote, quote_field_to_use),
                        "threshold_value": alert.threshold_value,
                        "creation_metadata": alert.creation_metadata,
                        "comparison_operator": alert.comparison_operator,
                        "quote_field_used": quote_field_to_use,
                    },
                )
        elif alert.comparison_operator == AlertComparisonOperator.LESS_THAN_EQUAL:
            if getattr(first_quote, "calculated_settlement" if (hasattr(first_quote,
                                                                        "calculated_settlement") and first_quote.calculated_settlement is not None) else quote_field_to_use) <= alert.threshold_value:
                alert_triggered(
                    db_session=session,
                    alert=alert,
                    event_metadata={
                        "trigger_value": getattr(first_quote, quote_field_to_use),
                        "threshold_value": alert.threshold_value,
                        "creation_metadata": alert.creation_metadata,
                        "comparison_operator": alert.comparison_operator,
                        "quote_field_used": quote_field_to_use,
                    },
                )

    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception(exc)
        self.retry(exc=exc, max_retries=5)


@celery_app.task(name="tasks.db_connection_monitoring")
def db_connection_monitoring():
    try:
        info = get_pool_info()
        logger.info(info)
        return info
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception(exc)
