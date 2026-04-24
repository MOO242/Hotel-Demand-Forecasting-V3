# Hotel KPI Definitions (Hotel Demand Forecasting V3)

These KPIs are calculated for a selected year using arrivals in that year (`arrival_date`).

## Date Logic

- `arrival_date` is derived from:
  - arrival_date_year, arrival_date_month, arrival_date_day_of_month
- Canceled bookings:
  - Included in cancellation rate denominator
  - Excluded from Rooms Sold and Room Revenue

---

## KPI Definitions

### Rooms Available

**Definition:** Total available room nights in the period  
**Formula:** days_in_period × capacity

### Rooms Sold (Room Nights Sold)

**Definition:** Total room nights sold excluding cancellations  
**Formula:** Σ(room_nights) for records where is_canceled = 0  
room_nights = stays_in_week_nights + stays_in_weekend_nights

### Room Revenue

**Definition:** Total room revenue excluding cancellations  
**Formula:** Σ(adr × room_nights) where is_canceled = 0

### Occupancy %

**Formula:** (Rooms Sold / Rooms Available) × 100

### ADR

**Formula:** Room Revenue / Rooms Sold

### RevPAR

**Formula:** Room Revenue / Rooms Available

### Cancellation %

**Formula:** (Canceled Bookings / Total Bookings) × 100

---

## Notes / Assumptions

- ADR is assumed to be per night.
- Capacity is passed as parameter and assumed constant across the year.
- This yearly KPI view is arrival-based. Daily occupancy requires exploding stays across nights.
