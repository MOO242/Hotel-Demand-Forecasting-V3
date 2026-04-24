CREATE
OR REPLACE VIEW vw_hotel_kpis AS
SELECT
    rs.hotel,
    rs.arrival_date_year,
    rs.rooms_sold,
    rr.room_revenue,
    adr.adr,
    ra.rooms_available,
    occ.occupancy,
    rp.revpar
FROM
    vw_rooms_sold rs
    LEFT JOIN vw_room_revenue rr ON rs.hotel = rr.hotel
    AND rs.arrival_date_year = rr.arrival_date_year
    LEFT JOIN vw_adr adr ON rs.hotel = adr.hotel
    AND rs.arrival_date_year = adr.arrival_date_year
    LEFT JOIN vw_rooms_available ra ON rs.hotel = ra.hotel
    AND rs.arrival_date_year = ra.arrival_date_year
    LEFT JOIN vw_occupancy occ ON rs.hotel = occ.hotel
    AND rs.arrival_date_year = occ.arrival_date_year
    LEFT JOIN vw_revpar rp ON rs.hotel = rp.hotel
    AND rs.arrival_date_year = rp.arrival_date_year;
