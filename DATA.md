# bbmp-trade-license

The source data is fetched from the [BBMP Trade License Website](https://trade.bbmpgov.in/Forms/frmApplicationStatusPublic.aspx)

## Data dictionary

| Variable | Type | Description |
|----------|------|-------------|
| Application Type | category | Type of trade license application (New or Renewal) |
| Date | datetime | Date of the trade license application |
| Application ID | int64 | Unique ID of the trade license application |
| Trade Name | string | Name of the business/trade |
| Trade Type | category | Type of trade |
| Major Trade | category | Major category of trade |
| Minor Trade | category | Minor category of trade |
| Sub Trade | category | Sub-category of trade |
| Paid Amount | float64 | Amount paid for the license |
| Penalty Amount | float64 | Penalty amount for the license |
| Total Paid Amount | float64 | Total amount paid including penalties |
| Constituency | category | Assembly constituency where the trade is located |
| Ward | category | BBMP ward where the trade is located |
| Address Door Number | string | Door number of the trade address |
| Address Street | string | Street name of the trade address |
| Address Area | string | Area/locality of the trade address |
| Address PIN | string | PIN code of the trade address |
| Zonal Classification | string | Zonal classification of the trade location |