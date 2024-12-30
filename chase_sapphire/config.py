dataset = "personal_finance"

sapphire_table = "sapphire_totals"

sapphire_schema = [{
  "name": "period_start",
  "type": "DATE",
  "mode": "NULLABLE"
}, {
  "name": "period_end",
  "type": "DATE",
  "mode": "NULLABLE"
}, {
  "name": "total",
  "type": "FLOAT",
  "mode": "NULLABLE"
}, {
  "name": "dt_updated",
  "type": "TIMESTAMP",
  "mode": "NULLABLE"
}]

repl_lst = ["Purchases", "+", "$", ","]

final_cols = ["period_start", "period_end", "total", "dt_updated"]
