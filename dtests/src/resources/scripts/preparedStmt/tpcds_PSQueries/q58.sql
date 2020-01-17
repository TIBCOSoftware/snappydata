WITH ss_items AS
(SELECT
    i_item_id item_id,
    sum(ss_ext_sales_price) ss_item_rev
  FROM store_sales, item, date_dim
  WHERE ss_item_sk = i_item_sk
    AND d_date IN (SELECT d_date
  FROM date_dim
  WHERE d_week_seq = (SELECT d_week_seq
  FROM date_dim
  WHERE d_date = ?))
    AND ss_sold_date_sk = d_date_sk
  GROUP BY i_item_id),
    cs_items AS
  (SELECT
    i_item_id item_id,
    sum(cs_ext_sales_price) cs_item_rev
  FROM catalog_sales, item, date_dim
  WHERE cs_item_sk = i_item_sk
    AND d_date IN (SELECT d_date
  FROM date_dim
  WHERE d_week_seq = (SELECT d_week_seq
  FROM date_dim
  WHERE d_date = ?))
    AND cs_sold_date_sk = d_date_sk
  GROUP BY i_item_id),
    ws_items AS
  (SELECT
    i_item_id item_id,
    sum(ws_ext_sales_price) ws_item_rev
  FROM web_sales, item, date_dim
  WHERE ws_item_sk = i_item_sk
    AND d_date IN (SELECT d_date
  FROM date_dim
  WHERE d_week_seq = (SELECT d_week_seq
  FROM date_dim
  WHERE d_date = ?))
    AND ws_sold_date_sk = d_date_sk
  GROUP BY i_item_id)
SELECT
  ss_items.item_id,
  ss_item_rev,
  ss_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 ss_dev,
  cs_item_rev,
  cs_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 cs_dev,
  ws_item_rev,
  ws_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 ws_dev,
  (ss_item_rev + cs_item_rev + ws_item_rev) / 3 average
FROM ss_items, cs_items, ws_items
WHERE ss_items.item_id = cs_items.item_id
  AND ss_items.item_id = ws_items.item_id
  AND ss_item_rev BETWEEN ? * cs_item_rev AND 1.1 * cs_item_rev
  AND ss_item_rev BETWEEN ? * ws_item_rev AND 1.1 * ws_item_rev
  AND cs_item_rev BETWEEN ? * ss_item_rev AND 1.1 * ss_item_rev
  AND cs_item_rev BETWEEN ? * ws_item_rev AND 1.1 * ws_item_rev
  AND ws_item_rev BETWEEN ? * ss_item_rev AND 1.1 * ss_item_rev
  AND ws_item_rev BETWEEN ? * cs_item_rev AND 1.1 * cs_item_rev
ORDER BY item_id, ss_item_rev
LIMIT 100
