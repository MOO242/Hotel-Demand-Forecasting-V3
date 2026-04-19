
# **Hotel-Demand-Forecasting-V3**

**Python → SQL & Python → Power BI**

This version focuses on building a **Hotel Performance KPI Engine** using Python and then recreating the same KPIs in **Power BI** for visualization and business insights.

The goal is to calculate the most important **commercial hotel KPIs** used in Revenue Management, Distribution, and Commercial Strategy.

---

##  **KPIs Included**

### **1. RevPAR (Revenue per Available Room)**  
\[
\text{RevPAR} = \text{Occupancy Rate} \times \text{ADR}
\]

Measures how well the hotel fills rooms *and* how much revenue it earns per room.

---

### **2. Total Hotel RevPAR**  
\[
\text{TotalHotelRevPAR} = \frac{\text{Total Revenue (Rooms + F&B + Other)}}{\text{Rooms Available}}
\]

A full commercial view beyond rooms revenue.

---

### **3. ADR (Average Daily Rate)**  
\[
\text{ADR} = \frac{\text{Room Revenue}}{\text{Rooms Sold}}
\]

Shows the average price paid per sold room.

---

### **4. Occupancy Rate**  
\[
\text{Occupancy} = \frac{\text{Rooms Sold}}{\text{Rooms Available}}
\]

Indicates how full the hotel is.

---

### **5. Total Revenue**  
Sum of all revenue generated from reservations:

- Weekday stays  
- Weekend stays  
- Room nights × ADR  

---

##  **Segmentation Analysis**

Segmentation shows **who** is booking the hotel.

\[
\text{Segmentation Revenue} = \sum(\text{Room Revenue by Segment})
\]

Typical segments:

- **Transient** (individual travelers)  
- **Group**  
- **Corporate / Contract**  
- **Leisure vs Business**  

---

##  **Distribution Channel Mix**

Shows **where** bookings come from.

\[
\text{Distribution Revenue} = \sum(\text{Room Revenue by Channel})
\]

Channels include:

- **Direct** (hotel website, call center)  
- **OTA** (Booking.com, Expedia)  
- **GDS** (corporate travel agencies)  
- **Voice / Offline**  

---

## 👤 **Customer Type Mix**

Shows **what type of customer** is booking.

Examples:

- Transient  
- Transient-Party  
- Group  
- Contract  

\[
\text{Customer Type Revenue} = \sum(\text{Room Revenue by Customer Type})
\]

---

## 💻 **Digital Mix**

Percentage of bookings coming from digital channels:

\[
\text{Digital Mix} = \frac{\text{Digital Bookings}}{\text{Total Bookings}} \times 100
\]

Digital channels include:

- Direct Web  
- OTA  
- Mobile  

Offline channels include:

- Voice  
- Walk‑ins  
- Travel agents (non-digital)

---
