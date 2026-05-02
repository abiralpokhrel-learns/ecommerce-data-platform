"""Mapping of source CSVs to target tables."""

INGESTION_CONFIG = {
    "olist_customers_dataset.csv": {
        "table": "raw.customers",
        "primary_key": "customer_id",
    },
    "olist_geolocation_dataset.csv": {
        "table": "raw.geolocation",
        "primary_key": None,  # No PK in this table
    },
    "olist_order_items_dataset.csv": {
        "table": "raw.order_items",
        "primary_key": ["order_id", "order_item_id"],
    },
    "olist_order_payments_dataset.csv": {
        "table": "raw.order_payments",
        "primary_key": ["order_id", "payment_sequential"],
    },
    "olist_order_reviews_dataset.csv": {
        "table": "raw.order_reviews",
        "primary_key": "review_id",
    },
    "olist_orders_dataset.csv": {
        "table": "raw.orders",
        "primary_key": "order_id",
    },
    "olist_products_dataset.csv": {
        "table": "raw.products",
        "primary_key": "product_id",
    },
    "olist_sellers_dataset.csv": {
        "table": "raw.sellers",
        "primary_key": "seller_id",
    },
    "product_category_name_translation.csv": {
        "table": "raw.product_category_translation",
        "primary_key": "product_category_name",
    },
}