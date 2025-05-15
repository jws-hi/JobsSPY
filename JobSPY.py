import pandas as pd
from datetime import datetime
import yfinance as yf
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt




# Download SPY data from December 1, 2023 to today
df = yf.download('SPY', start='2023-12-01', interval='1d')

# Only proceed if data is downloaded
if not df.empty:
    # Resample by month-end ('ME') and sum the Volume
    monthly_volume = df['Volume'].resample('ME').sum()

    # Format index to YYYY-MM format
    monthly_volume.index = monthly_volume.index.strftime('%Y-%m')
    monthly_volume_df = monthly_volume.reset_index()
    monthly_volume_df.columns = ['Date', 'Total_Volume']

    print(monthly_volume_df)
else:
    print("No data was downloaded. You may be rate limited. Try again in a few minutes.")







# Step 1: Load raw lines from the CSV file
file_path = "/Users/jws/Desktop/Level 1 Coding Projects/unemployementdata.csv"  # Change this path as needed
with open(file_path, 'r', encoding='utf-8') as file:
    raw_lines = file.readlines()

# Step 2: Extract and clean the correct header (months) and R4 data line
month_line = raw_lines[11]  # Line containing month headers
r4_line = raw_lines[16]     # Line containing "R4 - official rate 11"

# Step 3: Clean and split the data
months = [m.strip().strip('"') for m in month_line.strip().split(",")[1:]]  # skip the label
r4_values = [v.strip().strip('"') for v in r4_line.strip().split(",")[1:]]  # skip the label

# Step 4: Combine and filter the data from April 2024 to April 2025
r4_data = list(zip(months, r4_values))
start_index = months.index("December 2023")
end_index = months.index("April 2025") + 1
filtered_r4_data = r4_data[start_index:end_index]

# Step 5: Convert to a DataFrame for easy viewing or export
r4_df = pd.DataFrame(filtered_r4_data, columns=["Month", "R4 - Official Rate 11"])

# Step 6: Add a new column with the reformatted dates (YYYY-MM)
def convert_month_format(month_str):
    try:
        # Parse the month string into a datetime object
        date_obj = datetime.strptime(month_str, "%B %Y")
        # Format the datetime object into YYYY-MM
        return date_obj.strftime("%Y-%m")
    except ValueError:
        # Handle any parsing errors
        return month_str  # Return original if parsing fails

# Add the new column with YYYY-MM format
r4_df["Date"] = r4_df["Month"].apply(convert_month_format)

# Optionally rearrange columns to put Date first
r4_df = r4_df[["Date", "R4 - Official Rate 11"]]

# Optional: Display or save
print(r4_df)
# r4_df.to_csv("filtered_r4_data.csv", index=False)






# Connect or create a database file
conn = sqlite3.connect("joined_data.db")

# Write both DataFrames to SQL
monthly_volume_df.to_sql("monthly_volume", conn, if_exists="replace", index=False)
r4_df.to_sql("r4_rates", conn, if_exists="replace", index=False)

query = """
SELECT
    v.Date,
    v.Total_Volume,
    r.[R4 - Official Rate 11]
FROM 
    monthly_volume v
LEFT JOIN 
    r4_rates r
ON 
    v.Date = r.Date
"""

joined_df = pd.read_sql_query(query, conn)
print(joined_df)





# Ensure the R4 rate is numeric
joined_df["R4 - Official Rate 11"] = pd.to_numeric(joined_df["R4 - Official Rate 11"], errors="coerce")
joined_df["Total_Volume"] = pd.to_numeric(joined_df["Total_Volume"], errors="coerce")

# Drop any rows with missing values (optional)
plot_df = joined_df.dropna(subset=["Total_Volume", "R4 - Official Rate 11"])

# Create the plot
fig, ax1 = plt.subplots(figsize=(10, 6))

# First axis: Total Volume (bar chart)
ax1.bar(plot_df["Date"], plot_df["Total_Volume"], color='skyblue', label='Total SPY Volume')
ax1.set_ylabel("Total SPY Volume", color='skyblue')
ax1.tick_params(axis='y', labelcolor='skyblue')
ax1.set_xticks(plot_df["Date"])
ax1.set_xticklabels(plot_df["Date"], rotation=45, ha='right')

# Second axis: R4 rate (line chart)
ax2 = ax1.twinx()
ax2.plot(plot_df["Date"], plot_df["R4 - Official Rate 11"], color='darkred', marker='o', label='R4 Rate')
ax2.set_ylabel("R4 - Official Rate 11 (%)", color='darkred')
ax2.tick_params(axis='y', labelcolor='darkred')

# Title and layout
plt.title("SPY Monthly Volume vs. R4 Unemployment Rate")
fig.tight_layout()
plt.show()

