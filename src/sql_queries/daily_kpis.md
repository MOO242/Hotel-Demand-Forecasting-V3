# 🏨 Commercial Decision System

## Hotel KPI Engine — SQL Study Guide (PostgreSQL)

## Core Principle (Very Important)

All KPIs are calculated at the **same grain**:

    hotel + arrival_date_year

This consistency is what allows all KPIs to be joined safely.

## 1️ Rooms Sold

## Business Meaning

**Rooms Sold = total room nights actually stayed**  
(Cancellations excluded)

    rooms_sold = stays_in_week_nights + stays_in_weekend_nights

## SQL Logic

CREATE OR REPLACE VIEW vw_rooms_sold AS
SELECT
hotel,
arrival_date_year,
SUM(stays_in_week_nights + stays_in_weekend_nights) AS rooms_sold
FROM cleaned_hotel_data
WHERE is_canceled = 0
GROUP BY
hotel,
arrival_date_year;

### 🧠 Explanation

- `SUM(...)` → counts room nights
- `is_canceled = 0` → excludes canceled bookings
- Grouped by hotel + year

---

## 2️ Room Revenue

Room Revenue = ADR × Room Nights (non‑canceled)

## SQL Logic

CREATE OR REPLACE VIEW vw_room_revenue AS
SELECT
hotel,
arrival_date_year,
SUM(
adr \* (stays_in_week_nights + stays_in_weekend_nights)
) AS room_revenue
FROM cleaned_hotel_data
WHERE is_canceled = 0
GROUP BY
hotel,
arrival_date_year;

### Explanation

- Each booking’s revenue = `adr × room_nights`
- Summed per hotel/year
- Cancellations excluded

## 3️ ADR (Average Daily Rate)

ADR = Room Revenue ÷ Rooms Sold

This measures **price quality**, not demand.

---

## SQL Logic (Derived KPI – Best Practice)

CREATE OR REPLACE VIEW vw_adr AS
SELECT
rs.hotel,
rs.arrival_date_year,
rr.room_revenue / NULLIF(rs.rooms_sold, 0) AS adr
FROM vw_rooms_sold rs
JOIN vw_room_revenue rr
ON rs.hotel = rr.hotel
AND rs.arrival_date_year = rr.arrival_date_year;

- Built **from existing KPI views** (not raw data)
- `NULLIF(rooms_sold, 0)` prevents divide‑by‑zero
- Ensures KPI consistency across systems

---

## 4️ Rooms Available

Rooms Available = number of stay days × hotel capacity

This is purely about **supply**, not bookings.

## SQL Logic (Using `TO_DATE`)

CREATE OR REPLACE VIEW vw_rooms_available AS
SELECT
hotel,
arrival_date_year,
COUNT(
DISTINCT TO_DATE(
arrival_date_year || '-' ||
arrival_date_month || '-' ||
arrival_date_day_of_month,
'YYYY-Month-DD'
)
) \* 200 AS rooms_available
FROM cleaned_hotel_data
GROUP BY
hotel,
arrival_date_year;

Explanation

- `TO_DATE(...)` converts year+month+day into a real date
- `COUNT(DISTINCT ...)` counts unique stay days
- Multiplied by **capacity (200)**

## 5️ Occupancy %

Occupancy = Rooms Sold ÷ Rooms Available

## SQL Logic (Derived KPI)

CREATE OR REPLACE VIEW vw_occupancy AS
SELECT
rs.hotel,
rs.arrival_date_year,
rs.rooms_sold / NULLIF(ra.rooms_available, 0) AS occupancy
FROM vw_rooms_sold rs
JOIN vw_rooms_available ra
ON rs.hotel = ra.hotel
AND rs.arrival_date_year = ra.arrival_date_year;

- Combines **demand (sold)** with **supply (available)**
- Safe division using `NULLIF`
- Result is a decimal (×100 later for %)

## 6️ RevPAR (Revenue per Available Room)

RevPAR = Room Revenue ÷ Rooms Available

## SQL Logic

CREATE OR REPLACE VIEW vw_revpar AS
SELECT
rr.hotel,
rr.arrival_date_year,
rr.room_revenue / NULLIF(ra.rooms_available, 0) AS revpar
FROM vw_room_revenue rr
JOIN vw_rooms_available ra
ON rr.hotel = ra.hotel
AND rr.arrival_date_year = ra.arrival_date_year;

- Combines **pricing + demand + supply**
- RevPAR ≈ ADR × Occupancy (sanity check)

## 7️ Final Consolidated KPI Table (Single View)

## Purpose

One table that contains **ALL KPIs together**  
Perfect for Power BI, Python, and reporting.

## Final KPI Mart

CREATE OR REPLACE VIEW vw_hotel_kpis AS
SELECT
rs.hotel,
rs.arrival_date_year,

    rs.rooms_sold,
    rr.room_revenue,
    adr.adr,
    ra.rooms_available,
    occ.occupancy,
    rp.revpar

FROM vw_rooms_sold rs

LEFT JOIN vw_room_revenue rr
ON rs.hotel = rr.hotel
AND rs.arrival_date_year = rr.arrival_date_year

LEFT JOIN vw_adr adr
ON rs.hotel = adr.hotel
AND rs.arrival_date_year = adr.arrival_date_year

LEFT JOIN vw_rooms_available ra
ON rs.hotel = ra.hotel
AND rs.arrival_date_year = ra.arrival_date_year

LEFT JOIN vw_occupancy occ
ON rs.hotel = occ.hotel
AND rs.arrival_date_year = occ.arrival_date_year

LEFT JOIN vw_revpar rp
ON rs.hotel = rp.hotel
AND rs.arrival_date_year = rp.arrival_date_year;

Explanation

- Uses **LEFT JOINs** to preserve rows
- No KPI logic duplicated
- This is a **semantic KPI layer** (enterprise standard)

# Final Mental Model

    cleaned_hotel_data
            ↓
    Base KPIs:
      - vw_rooms_sold
      - vw_room_revenue
      - vw_rooms_available
            ↓
    Derived KPIs:
      - vw_adr
      - vw_occupancy
      - vw_revpar
            ↓
         FINAL KPI MART:
      - vw_hotel_kpis
