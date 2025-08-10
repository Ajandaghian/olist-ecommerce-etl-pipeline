# Raw Data analysis

process:
creation of custom functions to aviod repeating code
checking each CSV file for missing values, duplicates, and data types


#### Customers DataFrame Analysis
> ? what is the difference between customer id and customer unique id?

#### Geolocation DataFrame Analysis
OK

#### Orders DataFrame Analysis
- [ ] timestamps are not in datetime format, need to convert them
- [ ] check for missing values
    - [ ] in order approved
    - [ ] in order delivered carrier date
    *Shows the order posting timestamp. When it was handled to the logistic partner.*
    - [ ] in order delivered customer date
    *Shows the actual order delivery date to the customer.*

##### 🧹 Order Dataset – Null Values & Cleaning Summary

Several columns in the `orders` dataset contain `NaN` values that are **either expected based on the `order_status`** or indicate **incomplete or inconsistent data**. Here's how they were handled:

###### ✅ Expected Nulls (kept as-is):

* Orders with status `canceled`, `unavailable`, or `created` do not proceed to shipping →
  `order_approved_at`, `order_delivered_carrier_date`, and `order_delivered_customer_date` are logically `NaN`.
* Orders in `processing` or `invoiced` may not yet be shipped → delivery-related columns might be `NaN`.

###### ❌ Unexpected Nulls (cleaned or removed):
* Orders marked as `delivered` with missing `order_delivered_customer_date` or `order_delivered_carrier_date`
* Orders marked as `shipped` with missing `order_delivered_carrier_date`

###### 🛠 Fixes Applied:

* Removed inconsistent `delivered` rows missing delivery timestamps.
* Filled expected `NaN` values with `None` for canceled/unavailable/created orders.
* Converted all timestamp fields to datetime format for consistency.


#### Order Items DataFrame Analysis
OK

