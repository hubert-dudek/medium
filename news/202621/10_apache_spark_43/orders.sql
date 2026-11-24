SELECT line_number, product, quantity
FROM JSON_TABLE(
  '{"items":[{"name":"tea","qty":2},{"name":"coffee","qty":1}]}',
  '$.items[*]' COLUMNS (
    line_number FOR ORDINALITY,
    product STRING PATH '$.name',
    quantity INT PATH '$.qty'
  )
) AS order_items
ORDER BY line_number
