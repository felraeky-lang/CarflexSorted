import streamlit as st
import pandas as pd
from datetime import datetime , timezone
import requests ,json ,time , sqlite3 , os , re , ast
from bs4 import BeautifulSoup 
from io import BytesIO
import plotly.graph_objects as go

# ---------------- CONFIG ----------------
st.set_page_config(page_title="Car Listings App", layout="wide")
DB_FILE = os.path.join(os.path.dirname(__file__), "cars.db")


# ---------------- DATABASE ----------------
def clear_all_data(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    try:
        # Delete all rows
        c.execute("DELETE FROM autotrader")
        c.execute("DELETE FROM kjiji")

        # Reset autoincrement counters
        c.execute("DELETE FROM sqlite_sequence WHERE name='autotrader'")
        c.execute("DELETE FROM sqlite_sequence WHERE name='kjiji'")

        conn.commit()
        print("✅ All data cleared from 'autotrader' and 'kjiji' tables.")
    except Exception as e:
        print(f"❌ Error clearing data: {e}")
    finally:
        conn.close()

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS autotrader (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            price TEXT,
            location TEXT,
            odometer TEXT,
            image_src TEXT,
            ad_link TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS kjiji (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            name TEXT,
            description TEXT,
            image TEXT,
            price TEXT,
            priceCurrency TEXT,
            url TEXT UNIQUE,
            brand_name TEXT,
            mileage_value TEXT,
            mileage_unitCode TEXT,
            model TEXT,
            vehicleModelDate TEXT,
            bodyType TEXT,
            color TEXT,
            numberOfDoors TEXT,
            fuelType TEXT,
            vehicleTransmission TEXT,
            activationDate TEXT,
            sortingDate TEXT,
            time_since_activation,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()

def insert_car_autotreader(title, price, location, odometer, image_src,ad_link):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT INTO autotrader (title, price, location, odometer, image_src, ad_link, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (title, price, location, odometer, image_src, ad_link,now))
    conn.commit()
    conn.close()

def insert_car_kijiji(car):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("""
        INSERT OR IGNORE INTO kjiji (
            type, name, description, image, price, priceCurrency, url,
            brand_name, mileage_value, mileage_unitCode, model,
            vehicleModelDate, bodyType, color, numberOfDoors,
            fuelType, vehicleTransmission, activationDate, sortingDate, time_since_activation, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?)
    """, (
        str(car.get("type")),
        str(car.get("name")),
        str(car.get("Description")),   # <--- main suspect
        str(car.get("image")),
        str(car.get("price")),
        str(car.get("priceCurrency")),
        str(car.get("url")),
        str(car.get("brand_name")),
        str(car.get("mileage_value")),
        str(car.get("mileage_unitCode")),
        str(car.get("model")),
        str(car.get("vehicleModelDate")),
        str(car.get("bodyType")),
        str(car.get("color")),
        str(car.get("numberOfDoors")),
        str(car.get("fuelType")),
        str(car.get("vehicleTransmission")),
        str(car.get("activationDate")),
        str(car.get("sortingDate")),
        str(car.get("time_since_activation")),
        now
    ))
    conn.commit()
    conn.close()



def get_all_autotrader_cars():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM autotrader ORDER BY id ASC", conn)
    conn.close()
    return df

def get_all_kijiji_cars():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM kjiji ORDER BY id ASC", conn)
    conn.close()
    return df


def merge_car_data():
    kdf = get_all_kijiji_cars()
    adf = get_all_autotrader_cars()

    # Add a source column
    kdf["source"] = "Kijiji"
    adf["source"] = "Autotrader"

    # --- Kijiji normalization ---
    kdf = kdf.rename(columns={
        "name": "title",
        "priceCurrency": "currency",
        "brand_name": "brand",
        "mileage_value": "odometer",
        "url": "ad_link",
        "image": "image_src"
    })[[
        "source", "title", "price", "currency", "brand",
        "model", "vehicleModelDate", "bodyType", "color",
        "fuelType", "vehicleTransmission", "odometer",
        "image_src", "ad_link", "created_at"
    ]]

    # --- Autotrader normalization ---
    # Add missing columns if not present
    for col in ["title", "price", "odometer", "image_src", "ad_link", "created_at"]:
        if col not in adf.columns:
            adf[col] = None

    # Add optional fields missing from Autotrader
    for col in ["currency", "brand", "model", "vehicleModelDate", "bodyType", "color", "fuelType", "vehicleTransmission"]:
        adf[col] = None

    adf = adf[[
        "source", "title", "price", "currency", "brand",
        "model", "vehicleModelDate", "bodyType", "color",
        "fuelType", "vehicleTransmission", "odometer",
        "image_src", "ad_link", "created_at"
    ]]

    # Merge both
    merged = pd.concat([kdf, adf], ignore_index=True)
    merged.sort_values(by="created_at", ascending=False, inplace=True)
    return merged
    
def parse_kijiji_date1(date_str):
    if not date_str:
        return None
    try:
        # Format with timezone offset
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        try:
            # Format with trailing 'Z'
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    

def to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Merged Cars')
    return output.getvalue()
# Initialize the database
init_db()

# ---------------- SIDEBAR ----------------
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["📊 View Cars", "📝 Add Car"])

# ---------------- PAGE 1: VIEW ----------------
if page == "📊 View Cars":
    tokenTitle = st.text_input("Add your token", "enterprise-api.kdp.kardataservices")
    st.title("🚗 Autotrader Car Listings")
    with st.expander("See Autotrader explanation"):
        df = get_all_autotrader_cars()

        if df.empty:
            st.info("No cars found. Add new cars using the 'Add Car' page.")
        else:
            # Show DataFrame
            st.dataframe(df, use_container_width=True)

            # Card-style display
            for _, row in df.iterrows():
                with st.container():
                    cols = st.columns([1, 3])
                    with cols[0]:
                        if row['image_src'] != 'N/A':
                            st.image(row['image_src'], width=180)
                    with cols[1]:
                        st.subheader(row['title'])
                        
                        

                        brands = ['AM General','Acura','Alfa Romeo','American Motors (AMC)','Aston Martin','Audi','BMW','Bentley','BrightDrop','Buick','Cadillac','Chevrolet','Chrysler','Daewoo','Datsun','Dodge','Ducati','Eagle','FIAT','Ferrari','Fiat','Fisker','Ford','Freightliner','GMC','Genesis','Geo','HUMMER','Harley-Davidson','Hino','Honda','Hyundai','INEOS','INFINITI','Indian','International','Isuzu','Jaguar','Jeep','KTM','Karma','Kawasaki','Kenworth','Kia','Lamborghini','Land Rover','Lexus','Lincoln','Lordstown','Lotus','Lucid','MINI','MV-1','Mack','Maserati','Maybach','Mazda','McLaren','Mercedes-Benz','Mercury','Merkur','Mitsubishi','Moto Guzzi','Nissan','Oldsmobile','Panoz','Peterbilt','Peugeot','Plymouth','Polestar','Pontiac','Porsche','Ram','Renault','Rivian','Rolls-Royce','Saab','Saturn','Scion','Smart','Sterling','Subaru','Suzuki','Tesla','Toyota','Triumph','VPG','Victory','VinFast','Volkswagen','Volvo','Western Star','Yamaha','Yugo','Zero','smart']
                        matches = [brand for brand in brands if brand.lower() in row['title'].lower()]
                        if tokenTitle:
                            match = re.search(r'Authorization:\s*Bearer\s+([A-Za-z0-9\-\._]+)', tokenTitle)
                            if match:
                                token = match.group(1)
                                print("====================")
                                print(token)
                                
                                if st.button(f"{row['id']} - get market guide - {row['title'].lower()}"):
                                    
                                
                                    headers = {
                                        'Host': 'enterprise-api.kdp.kardataservices.com',
                                        'Sec-Ch-Ua-Platform': '"Windows"',
                                        'Authorization': f'Bearer {token}',
                                        'Accept-Language': 'en-US,en;q=0.9',
                                        'Sec-Ch-Ua': '"Chromium";v="141", "Not?A_Brand";v="8"',
                                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                                        'Sec-Ch-Ua-Mobile': '?0',
                                        'Accept': '*/*',
                                        'Origin': 'https://app.openlane.ca',
                                        'Sec-Fetch-Site': 'cross-site',
                                        'Sec-Fetch-Mode': 'cors',
                                        'Sec-Fetch-Dest': 'empty',
                                        'Referer': 'https://app.openlane.ca/',
                                        # 'Accept-Encoding': 'gzip, deflate, br',
                                        'Priority': 'u=1, i',
                                    }

                                    params = {
                                        'yearMin': '1940',
                                        'yearMax': '2027',
                                        'makeNames': f'{str(matches[0])}',
                                    }

                                    marketresponse = requests.get(
                                        'https://enterprise-api.kdp.kardataservices.com/vehicle-retail-data/marketguide/models',
                                        params=params,
                                        headers=headers,
                                        verify=False,
                                    )


                                    mdata = json.loads(marketresponse.text)['modelNames']
                                    model = [brand for brand in mdata if brand.lower() in row['title'].lower()][0]

                                    years = re.findall(r'\b(?:19|20)\d{2}\b', row['title'].lower())
                                    if years:
                                        year = str(int(years[0])-1)
                                        
                                        odometerMax = re.search(r'[\d,]+', row['odometer'])
                                        if odometerMax:
                                            num_int = int(odometerMax.group(0).replace(',', '')) + 5000
                                        params = {
                                            'teamId': 'ompProd',
                                            'makeNames': f'{str(matches[0])}',
                                            'modelNames': f'{str(model)}',
                                            'yearMin': f'{year}',
                                            'yearMax': '2027',
                                            'odometerMin': '0',
                                            'odometerMax': f'{str(num_int)}',
                                            'saleDateFrom': '2025-08-02',
                                            'saleDateTo': '2025-10-31',
                                            'sortBy': 'sale_date',
                                            'sortOrder': 'desc',
                                            'page': '0',
                                            'size': '10',
                                            'countryCode': 'CA',
                                            'organizationId': 'a10514a4-a594-4736-bcc8-3978ec88145a',
                                        }

                                        finalresponse = requests.get(
                                            'https://enterprise-api.kdp.kardataservices.com/vehicle-retail-data/marketguide',
                                            params=params,
                                            headers=headers,
                                            verify=False,
                                        )
                                        data = json.loads(finalresponse.text)
                                        del data['marketGuideVehicles']
                                        st.write(data)
                            else:
                                st.write(f"no token !!!")
                            
                        else:
                            st.write(f"no token !!!")




                        st.write(f"**Price:** {row['price']}")
                        st.write(f"**Location:** {row['location']}")
                        st.write(f"**Odometer:** {row['odometer']}")
                        st.caption(f"🕒 Added on: {row['created_at']}")
                        st.markdown(f"[🔗 View Ad]({row['ad_link']})", unsafe_allow_html=True)
                    st.divider()

            # Download CSV

            excel_data = to_excel_bytes(df)
            st.download_button(
                label="📊 Download autotreader Excel",
                data=excel_data,
                file_name="autotreader.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Download CSV",
                data=csv,
                file_name="cars.csv",
                mime="text/csv",
            )
    with st.expander("See Kijiji Vehicles"):
        kdf = get_all_kijiji_cars()

        if kdf.empty:
            st.info("🚗 No Kijiji cars found. Add new cars or scrape data first.")
        else:
            # Display DataFrame overview
            st.dataframe(kdf, use_container_width=True)

            # Card-style view
            for _, row in kdf.iterrows():
                with st.container():
                    cols = st.columns([1, 3])

                    # Left column — image
                    with cols[0]:
                        if row["image"]:
                            try:
                                # Try to parse as a Python list
                                parsed = ast.literal_eval(row["image"])
                                if isinstance(parsed, list) and parsed:
                                    first_image = parsed[0]   # ✅ first URL in list
                                elif isinstance(parsed, str) and parsed.startswith("http"):
                                    first_image = parsed
                            except Exception:
                                # Fallback: sometimes it's already a URL string
                                if isinstance(row["image"], str) and row["image"].startswith("http"):
                                    first_image = row["image"]
                        
                            st.image(first_image, width=180)
                            activation = parse_kijiji_date1(row['activationDate'] )
                            now = datetime.now(timezone.utc)
                            time_since_activation = (now - activation) if activation else None
                            st.write(f" { time_since_activation or 'N/A'} ⏱️")
                        else:
                            st.image("https://via.placeholder.com/180x120?text=No+Image", width=180)
                            activation = parse_kijiji_date1(row['activationDate'] )
                            now = datetime.now(timezone.utc)
                            time_since_activation = (now - activation) if activation else None
                            st.write(f" { time_since_activation or 'N/A'} ⏱️")



                    # Right column — vehicle details
                    with cols[1]:
                        st.subheader(row["name"] or "Unknown Vehicle")



                        brands = ['AM General','Acura','Alfa Romeo','American Motors (AMC)','Aston Martin','Audi','BMW','Bentley','BrightDrop','Buick','Cadillac','Chevrolet','Chrysler','Daewoo','Datsun','Dodge','Ducati','Eagle','FIAT','Ferrari','Fiat','Fisker','Ford','Freightliner','GMC','Genesis','Geo','HUMMER','Harley-Davidson','Hino','Honda','Hyundai','INEOS','INFINITI','Indian','International','Isuzu','Jaguar','Jeep','KTM','Karma','Kawasaki','Kenworth','Kia','Lamborghini','Land Rover','Lexus','Lincoln','Lordstown','Lotus','Lucid','MINI','MV-1','Mack','Maserati','Maybach','Mazda','McLaren','Mercedes-Benz','Mercury','Merkur','Mitsubishi','Moto Guzzi','Nissan','Oldsmobile','Panoz','Peterbilt','Peugeot','Plymouth','Polestar','Pontiac','Porsche','Ram','Renault','Rivian','Rolls-Royce','Saab','Saturn','Scion','Smart','Sterling','Subaru','Suzuki','Tesla','Toyota','Triumph','VPG','Victory','VinFast','Volkswagen','Volvo','Western Star','Yamaha','Yugo','Zero','smart']
                        matches = [brand for brand in brands if brand.lower() in row['name'].lower()]
                        if tokenTitle:
                            match = re.search(r'Authorization:\s*Bearer\s+([A-Za-z0-9\-\._]+)', tokenTitle)
                            if match:
                                token = match.group(1)
                                print("====================")
                                print(token)
                                
                                if st.button(f"{row['id']} - get market guide - {row['name'].lower()}"):
                                    
                                
                                    headers = {
                                        'Host': 'enterprise-api.kdp.kardataservices.com',
                                        'Sec-Ch-Ua-Platform': '"Windows"',
                                        'Authorization': f'Bearer {token}',
                                        'Accept-Language': 'en-US,en;q=0.9',
                                        'Sec-Ch-Ua': '"Chromium";v="141", "Not?A_Brand";v="8"',
                                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                                        'Sec-Ch-Ua-Mobile': '?0',
                                        'Accept': '*/*',
                                        'Origin': 'https://app.openlane.ca',
                                        'Sec-Fetch-Site': 'cross-site',
                                        'Sec-Fetch-Mode': 'cors',
                                        'Sec-Fetch-Dest': 'empty',
                                        'Referer': 'https://app.openlane.ca/',
                                        # 'Accept-Encoding': 'gzip, deflate, br',
                                        'Priority': 'u=1, i',
                                    }

                                    params = {
                                        'yearMin': '1940',
                                        'yearMax': '2027',
                                        'makeNames': f'{str(matches[0])}',
                                    }

                                    marketresponse = requests.get(
                                        'https://enterprise-api.kdp.kardataservices.com/vehicle-retail-data/marketguide/models',
                                        params=params,
                                        headers=headers,
                                        verify=False,
                                    )


                                    mdata = json.loads(marketresponse.text)['modelNames']
                                    
                                    model = [brand for brand in mdata if brand.lower() in row['name'].lower()]
                                    if model:

                                        years = re.findall(r'\b(?:19|20)\d{2}\b', row['name'].lower())
                                        if years:
                                            year = str(int(years[0])-1)
                                            
                                            odometerMax = re.search(r'[\d,]+', row['mileage_value'])
                                            if odometerMax:
                                                num_int = int(odometerMax.group(0).replace(',', '')) + 5000
                                            params = {
                                                'teamId': 'ompProd',
                                                'makeNames': f'{str(matches[0])}',
                                                'modelNames': f'{str(model[0])}',
                                                'yearMin': f'{year}',
                                                'yearMax': '2027',
                                                'odometerMin': '0',
                                                'odometerMax': f'{str(num_int)}',
                                                'saleDateFrom': '2025-08-02',
                                                'saleDateTo': '2025-10-31',
                                                'sortBy': 'sale_date',
                                                'sortOrder': 'desc',
                                                'page': '0',
                                                'size': '10',
                                                'countryCode': 'CA',
                                                'organizationId': 'a10514a4-a594-4736-bcc8-3978ec88145a',
                                            }

                                            finalresponse = requests.get(
                                                'https://enterprise-api.kdp.kardataservices.com/vehicle-retail-data/marketguide',
                                                params=params,
                                                headers=headers,
                                                verify=False,
                                            )
                                            data = json.loads(finalresponse.text)
                                            del data['marketGuideVehicles']
                                            ############################################################
                                            ############################################################
                                            ############################################################
                                            ########################################################################################################################
                                            
                                            ############################################################
                                            ############################################################
                                            ############################################################
                                            ############################################################
                                            ############################################################
                                            
                                            fig = go.Figure(go.Indicator(
                                            mode="gauge+number+delta",
                                            value=int(row['price']),
                                            title={'text': f" Market Guide Vehicles For {row['model']} "},
                                            delta={'reference': int(data['priceAggAve'])},
                                            gauge={
                                                'axis': {'range': [int(data['priceAggMin']), int(data['priceAggMax'])]},
                                                'bar': {'color': 'red'},
                                                'steps': [
                                                    {'range': [int(data['priceAggMin']), int(data['priceAggAve'])], 'color': "lightgreen"},
                                                    {'range': [data['priceAggAve'], data['priceAggMax']], 'color': "lightcoral"}
                                                ],
                                                'threshold': {'line': {'color': "black", 'width': 4}, 'value': int(data['priceAggAve'])}
                                                    }
                                                ))

                                            fig.update_layout(height=300)
                                            st.plotly_chart(fig, use_container_width=True)
                                        
                                            st.write(data)
                                        else:
                                            st.warning(f"no year matched !!!")
                                    else:
                                            st.warning(f"no model matched !!!")
                                
                                
                            else:
                                st.write(f"no token !!!")






                        st.write(f"**Type:** {row['type'] or 'N/A'}")
                        st.write(f"**Model:** {row['model'] or 'N/A'} ({row['vehicleModelDate'] or 'N/A'})")
                        st.write(f"**Price:** {row['price'] or 'N/A'} {row['priceCurrency'] or ''}")
                        st.write(f"**Brand:** {row['brand_name'] or 'N/A'}")
                        st.write(f"**Body Type:** {row['bodyType'] or 'N/A'}")
                        st.write(f"**Color:** {row['color'] or 'N/A'}")
                        st.write(f"**Fuel Type:** {row['fuelType'] or 'N/A'}")
                        st.write(f"**Transmission:** {row['vehicleTransmission'] or 'N/A'}")

                        st.markdown("---")
                        
                        st.write(f"**Mileage:** {row['mileage_value'] or 'N/A'} {row['mileage_unitCode'] or ''}")
                        st.write(f"**Doors:** {row['numberOfDoors'] or 'N/A'}")
                        st.caption(f"🕒 Added on: {row['created_at']}")

                        if row["url"]:
                            st.markdown(f"[🔗 View Ad]({row['url']})", unsafe_allow_html=True)

                    st.divider()

            # --- Download button ---
            excel_data = to_excel_bytes(kdf)
            st.download_button(
                label="📊 Download kjiji Excel",
                data=excel_data,
                file_name="kijiji_cars.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            csv = kdf.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Download All Cars (CSV)",
                data=csv,
                file_name="kijiji_cars.csv",
                mime="text/csv",
            )       
    with st.expander("🧩 Combined View: Kijiji + Autotrader"):
        merged_df = merge_car_data()

        if merged_df.empty:
            st.info("No cars found in either table.")
        else:
            st.dataframe(merged_df, use_container_width=True)

            # Card-style display
            for _, row in merged_df.iterrows():
                with st.container():
                    cols = st.columns([1, 3])
                    with cols[0]:
                        if row["image_src"]:
                            try:
                                # Try to parse as a Python list
                                parsed = ast.literal_eval(row["image_src"])
                                if isinstance(parsed, list) and parsed:
                                    first_image = parsed[0]   # ✅ first URL in list
                                elif isinstance(parsed, str) and parsed.startswith("http"):
                                    first_image = parsed
                            except Exception:
                                # Fallback: sometimes it's already a URL string
                                if isinstance(row["image_src"], str) and row["image_src"].startswith("http"):
                                    first_image = row["image_src"]
                    
                            st.image(first_image, width=180)
                        else:
                            st.image("https://via.placeholder.com/180x120?text=No+Image", width=180)
                    with cols[1]:
                        st.subheader(row["title"] or "Unknown Vehicle")
                        st.caption(f"📦 Source: {row['source']}")
                        st.write(f"**Price:** {row['price'] or 'N/A'} {row['currency'] or ''}")
                        st.write(f"**Brand:** {row['brand'] or 'N/A'}")
                        st.write(f"**Model:** {row['model'] or 'N/A'} ({row['vehicleModelDate'] or 'N/A'})")
                        st.write(f"**Body Type:** {row['bodyType'] or 'N/A'}")
                        st.write(f"**Color:** {row['color'] or 'N/A'}")
                        st.write(f"**Fuel Type:** {row['fuelType'] or 'N/A'}")
                        st.write(f"**Transmission:** {row['vehicleTransmission'] or 'N/A'}")
                        st.write(f"**Odometer:** {row['odometer'] or 'N/A'}")
                        st.caption(f"🕒 Added on: {row['created_at']}")
                        if row["ad_link"]:
                            st.markdown(f"[🔗 View Ad]({row['ad_link']})", unsafe_allow_html=True)
                    st.divider()

            # Excel Download
            excel_data = to_excel_bytes(merged_df)
            st.download_button(
                label="📊 Download Combined Excel",
                data=excel_data,
                file_name="merged_cars.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
# ---------------- PAGE 2: ADD ----------------
elif page == "📝 Add Car":
    st.title("📝 Add New Car Listing")

    AutotraderSubmitted = st.button("Updata Autotrader Car")
    KjijiSubmitted = st.button("Updata Kjiji Car")
    clear = st.button("reset all data (clear all)")
    if clear:
        clear_all_data(DB_FILE)
        st.success("✅ Reset Done! ")
    if KjijiSubmitted:

        cookies = {
            'kjses': 'a3ada55c-3dda-4d3b-a2f1-5a2dc3e6d11e^MSym5/LO9nctRVl8JS0kFA==',
            'machId': '22fb321cba3b00c1b9e5ec088612772657052a66147091639177d4bb1d9b30c7619ed61ccc0c45ded10273971642021362cab9ba47cc83305e4d338bf26682f3',
            'up': '%7B%22ln%22%3A%22725948023%22%2C%22ls%22%3A%22sv%3DLIST%26sf%3DdateDesc%22%7D',
        }

        headers = {
            'Host': 'www.kijiji.ca',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Accept-Language': 'en-US,en;q=0.9',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Priority': 'u=0, i',
            # 'Cookie': 'kjses=a3ada55c-3dda-4d3b-a2f1-5a2dc3e6d11e^MSym5/LO9nctRVl8JS0kFA==; machId=22fb321cba3b00c1b9e5ec088612772657052a66147091639177d4bb1d9b30c7619ed61ccc0c45ded10273971642021362cab9ba47cc83305e4d338bf26682f3; up=%7B%22ln%22%3A%22725948023%22%2C%22ls%22%3A%22sv%3DLIST%26sf%3DdateDesc%22%7D',
        }

        params = {
            'for-sale-by': 'ownr',
            'price': '0__',
            'view': 'list',
        }
        flagKjiji = False
        kresponse = requests.get(
            'https://www.kijiji.ca/b-cars-trucks/ontario/c174l9004',
            params=params,
            cookies=cookies,
            headers=headers,
            verify=False,
        )
        if kresponse.status_code != 200:
            st.warning("⚠️ Attempt failed. Retrying in 60 seconds...")

            time.sleep(60)
            for attempt in range(3):
                kresponse = requests.get(
                    'https://www.kijiji.ca/b-cars-trucks/ontario/c174l9004',
                    params=params,
                    cookies=cookies,
                    headers=headers,
                    verify=False,
                )
                if kresponse.status_code == 200:
                    st.success("✅ Done! Kijiji Cars successfully connected in the api.")
                    flagKjiji = True
                    break
                st.warning(f"⚠️ Attempt failed {attempt + 1}. Retrying in 60 seconds...")
                
                time.sleep(60)
        else:
            flagKjiji = True
            st.success("✅ Done! Kijiji Cars successfully connected in the api.")
        
        if flagKjiji == True:
           
            html = kresponse.text

            
            match = re.search(r'<script[^>]+type="application/json"[^>]*>(.*?)</script>', html, re.DOTALL)
            if match:
                json_text = match.group(1).strip()
                json_text = json_text.replace('&quot;', '"').replace('&amp;', '&')
                data = json.loads(json_text)
            else:
                raise Exception("Could not find embedded JSON")

            # Recursive search for "AutosListing:"
            def find_autos_listings(obj):
                results = {}
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k.startswith("AutosListing:"):
                            results[k] = v
                        else:
                            results.update(find_autos_listings(v))
                elif isinstance(obj, list):
                    for item in obj:
                        results.update(find_autos_listings(item))
                return results

            listings = find_autos_listings(data)
          
            st.success(f"Found {len(listings)} listings")
            # Parse ISO date
            def parse_kijiji_date(date_str):
                if not date_str:
                    return None
                try:
                    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
                except ValueError:
                    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

            # Current UTC time
            now = datetime.now(timezone.utc)

            # Collect all listings
            all_listings = []
            for key, listing in listings.items():
                attributes = listing.get("attributes", {}).get("all", [])

                def get_attr(name):
                    for attr in attributes:
                        if attr.get("canonicalName") == name:
                            return attr.get("canonicalValues", [None])[0]
                    return None

                activation = parse_kijiji_date(listing.get("activationDate"))
                sorting = parse_kijiji_date(listing.get("sortingDate"))

                # Calculate time since activation and difference between sorting and activation
                time_since_activation = (now - activation) if activation else None
                sorting_diff = (sorting - activation) if (sorting and activation) else None

                all_listings.append({
                    "type": listing.get("__typename"),
                    "name": listing.get("title"),
                    "Description": listing.get("description"),
                    "image": listing.get("imageUrls", [None]),
                    "price": listing.get("price", {}).get("amount"),
                    "priceCurrency": "CAD",
                    "url": listing.get("url"),
                    "brand_name": get_attr("carmake"),
                    "mileage_value": get_attr("carmileageinkms"),
                    "mileage_unitCode": "KMT",
                    "model": get_attr("carmodel"),
                    "vehicleModelDate": get_attr("caryear"),
                    "bodyType": get_attr("carbodytype"),
                    "color": get_attr("carcolor"),
                    "numberOfDoors": get_attr("noofdoors"),
                    "fuelType": get_attr("carfueltype"),
                    "vehicleTransmission": get_attr("cartransmission"),
                    "activationDate": activation.isoformat() if activation else None,
                    "sortingDate": sorting.isoformat() if sorting else None,
                    "time_since_activation": str(time_since_activation) if time_since_activation else None,
                    "activation_to_sorting_diff": str(sorting_diff) if sorting_diff else None
                })

            # Sort by activationDate (newest first)
            sorted_listings = sorted(all_listings, key=lambda x: x["activationDate"] or datetime.min, reverse=True)

            for listing in sorted_listings:
                insert_car_kijiji(listing)

            st.success("✅ Done! Kjiji Cars successfully added to the database.")




    if AutotraderSubmitted:
   

        cookies = {
            'as24Visitor': 'c3c760d9-0878-408d-a19b-2180d1931375',
            'ab_test_lp': '%7B%22abTest740ComparisonFeature%22%3A%22abtest-740_variation_a%22%7D',
            'visid_incap_820541': 'PKiQE+rTTnqperHoTPBa4tLDEWkAAAAAQUIPAAAAAADlg8tQIlw7MRuZnv64x+pW',
            'nlbi_820541_3122371': '5Qbyek4Vd00I5av4pRL4bAAAAAA9PPLlUlcN+ZpBnL9/m2b9',
            'incap_ses_1776_820541': 'uW+LcvEvkjXjMEPdFJ+lGNPDEWkAAAAAlBI2pxFRiGU/kcvWgd8hPg==',
            'nlbi_820541_3120041': 'pmRoSFKMVAJRVvvOpRL4bAAAAADfZdw2B4NIGxW0/vooNruv',
            'nlbi_820541_3127200': '1mNKGNNWJya6qtEdpRL4bAAAAABvgkYRJ8QHpATlKN0ZRp6c',
            'culture': 'en-CA',
            'fallback-zip': '%7B%22label%22%3A%22N5X0E2%20London%2C%20ON%22%2C%22lat%22%3A43.029117584228516%2C%22lon%22%3A-81.26272583007812%7D',
            'nlbi_820541_3163786': 'jHOsYtqQaQfhsKOFpRL4bAAAAADh1E3Y04D6Lc6xys2DCcl6',
            'as24-gtmSearchCrit': '0010001-0020000:cc|cy|rn|cu',
            'at_as24_site_exp': 'onemp',
            '_cq_duid': '1.1762771930.cdR0NwW6AZPpu8aK',
            '_cq_suid': '1.1762771930.MoGQGNTaKdpzGwnI',
            '_gcl_au': '1.1.1886041771.1762771930',
            '_ga': 'GA1.1.83325848.1762771931',
            'FPID': 'FPID2.2.2IqdjODDjA3wGmtH4ZCNeesz9Gjk23y%2FPi3uMiuMmoI%3D.1762771931',
            'FPLC': 'AW%2FNnyvZubMKO1%2FcghoA01cRyrAQ8iyffLogr3pk6IX%2B%2BV7rkhW5%2B7MA3AcRI7CI9lOyOaI1Xd3icyFAuEH%2B2PqS%2FbE4A9vJH%2B%2FR6OesPvOLJKnb21uz8YHqc%2F4pcA%3D%3D',
            'FPAU': '1.1.1886041771.1762771930',
            '_gtmeec': 'e30%3D',
            '_fbp': 'fb.1.1762771930982.1774960817',
            'nlbi_820541_3156894': 'cK7GZnlkmFBu4USEpRL4bAAAAABXW0bE1BNblaqCh8sJN0Ca',
            '__T2CID__': 'eb28b989-4140-452d-ac76-75c851c5f553',
            '_clck': 'v16eb9%5E2%5Eg0w%5E0%5E2140',
            '_cc_id': '853619dbd287e54c74355720e04e8ef7',
            'panoramaId': '1506e8abdd2ecdef53769d165cfea9fb927aad097ec22e82e8edcadb259f59ca',
            'panoramaIdType': 'panoDevice',
            'cc_audpid': '853619dbd287e54c74355720e04e8ef7',
            'nlbi_820541_3181253': 'Rsj2QfBuZB3xaiPxpRL4bAAAAABiz5XD8ZEuqjsmv5JIS5pp',
            '___iat_ses': '5474ECF60E041E03',
            'cbnr': '1',
            '__gads': 'ID=56760f4de69aba99:T=1762771964:RT=1762771964:S=ALNI_MY4KTjzzmGwdjMdJAwTuJNiFkIs-A',
            '__gpi': 'UID=000012c7020b9b88:T=1762771964:RT=1762771964:S=ALNI_MZrhXe8JdXVj_z1BschzbBh3W2k6g',
            '__eoi': 'ID=ef85606016a43971:T=1762771964:RT=1762771964:S=AA-AfjZ1d4CgH4oLZI_zGwocyQTS',
            '_asse': 'cm:eyJzbSI6WyIxfDE3NjI3NzE5Mjk2NjJ8MHwwfDM5MTA0fG4iLDE4MjU4NDM5Njg3NjZdfQ==',
            '_uetsid': '50f7c550be2311f0940e9bfe639c74a4',
            '_uetvid': '50f7e640be2311f080c395e793823310',
            '_cq_pxg': '3|p7540524170993107026644946499',
            'FCCDCF': '%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%226d740418-bc8a-4f2a-bd34-bf1c2ecf5a85%5C%22%2C%5B1762771964%2C96000000%5D%5D%22%5D%5D%5D',
            'FCNEC': '%5B%5B%22AKsRol-5W8HZTX4B77SYl3I8RPx6vsdRq-o5IStvZsI-6goTReUCK5zkW1N-2I2eJv-UppQNQxOlM9z6oAEQr6WeCCPwNhMiJq06SplepUnGNCzzAfpPVqUGRaDGODvxhnMO2aHzLvt_4OYVUkhJOIxx7FsJO_HsSA%3D%3D%22%5D%5D',
            'last-search-feed': 'atype%3DC%26custtype%3DP%26cy%3DCA%26damaged_listing%3Dexclude%26desc%3D1%26lat%3D43.029117584228516%26lon%3D-81.26272583007812%26offer%3DU%26size%3D40%26sort%3Dage%26ustate%3DN%252CU%26zip%3DN5X0E2%2520London%252C%2520ON%26zipr%3D1000',
            '_ga_YKMVVRSW3Y': 'GS2.1.s1762771931$o1$g1$t1762772105$j10$l0$h0',
            '_ga_TX2QRVWP93': 'GS2.1.s1762771931$o1$g1$t1762772105$j60$l0$h987418783',
            '___iat_vis': '5474ECF60E041E03.d365e08ebc04d931d71f0f6e5c8b9051.1762772106907.6f92ff3b6ad04d7a6960250481c51d7a.ROAIMMMOOZ.11111111.1-0.d365e08ebc04d931d71f0f6e5c8b9051',
            '_clsk': '1husmi%5E1762772107776%5E3%5E0%5Eo.clarity.ms%2Fcollect',
            'panoramaId_expiry': '1762858506944',
        }

        headers = {
            'Host': 'www.autotrader.ca',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Not_A Brand";v="99", "Chromium";v="142"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Accept-Language': 'en-US,en;q=0.9',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Priority': 'u=0, i',
            # 'Cookie': 'as24Visitor=c3c760d9-0878-408d-a19b-2180d1931375; ab_test_lp=%7B%22abTest740ComparisonFeature%22%3A%22abtest-740_variation_a%22%7D; visid_incap_820541=PKiQE+rTTnqperHoTPBa4tLDEWkAAAAAQUIPAAAAAADlg8tQIlw7MRuZnv64x+pW; nlbi_820541_3122371=5Qbyek4Vd00I5av4pRL4bAAAAAA9PPLlUlcN+ZpBnL9/m2b9; incap_ses_1776_820541=uW+LcvEvkjXjMEPdFJ+lGNPDEWkAAAAAlBI2pxFRiGU/kcvWgd8hPg==; nlbi_820541_3120041=pmRoSFKMVAJRVvvOpRL4bAAAAADfZdw2B4NIGxW0/vooNruv; nlbi_820541_3127200=1mNKGNNWJya6qtEdpRL4bAAAAABvgkYRJ8QHpATlKN0ZRp6c; culture=en-CA; fallback-zip=%7B%22label%22%3A%22N5X0E2%20London%2C%20ON%22%2C%22lat%22%3A43.029117584228516%2C%22lon%22%3A-81.26272583007812%7D; nlbi_820541_3163786=jHOsYtqQaQfhsKOFpRL4bAAAAADh1E3Y04D6Lc6xys2DCcl6; as24-gtmSearchCrit=0010001-0020000:cc|cy|rn|cu; at_as24_site_exp=onemp; _cq_duid=1.1762771930.cdR0NwW6AZPpu8aK; _cq_suid=1.1762771930.MoGQGNTaKdpzGwnI; _gcl_au=1.1.1886041771.1762771930; _ga=GA1.1.83325848.1762771931; FPID=FPID2.2.2IqdjODDjA3wGmtH4ZCNeesz9Gjk23y%2FPi3uMiuMmoI%3D.1762771931; FPLC=AW%2FNnyvZubMKO1%2FcghoA01cRyrAQ8iyffLogr3pk6IX%2B%2BV7rkhW5%2B7MA3AcRI7CI9lOyOaI1Xd3icyFAuEH%2B2PqS%2FbE4A9vJH%2B%2FR6OesPvOLJKnb21uz8YHqc%2F4pcA%3D%3D; FPAU=1.1.1886041771.1762771930; _gtmeec=e30%3D; _fbp=fb.1.1762771930982.1774960817; nlbi_820541_3156894=cK7GZnlkmFBu4USEpRL4bAAAAABXW0bE1BNblaqCh8sJN0Ca; __T2CID__=eb28b989-4140-452d-ac76-75c851c5f553; _clck=v16eb9%5E2%5Eg0w%5E0%5E2140; _cc_id=853619dbd287e54c74355720e04e8ef7; panoramaId=1506e8abdd2ecdef53769d165cfea9fb927aad097ec22e82e8edcadb259f59ca; panoramaIdType=panoDevice; cc_audpid=853619dbd287e54c74355720e04e8ef7; nlbi_820541_3181253=Rsj2QfBuZB3xaiPxpRL4bAAAAABiz5XD8ZEuqjsmv5JIS5pp; ___iat_ses=5474ECF60E041E03; cbnr=1; __gads=ID=56760f4de69aba99:T=1762771964:RT=1762771964:S=ALNI_MY4KTjzzmGwdjMdJAwTuJNiFkIs-A; __gpi=UID=000012c7020b9b88:T=1762771964:RT=1762771964:S=ALNI_MZrhXe8JdXVj_z1BschzbBh3W2k6g; __eoi=ID=ef85606016a43971:T=1762771964:RT=1762771964:S=AA-AfjZ1d4CgH4oLZI_zGwocyQTS; _asse=cm:eyJzbSI6WyIxfDE3NjI3NzE5Mjk2NjJ8MHwwfDM5MTA0fG4iLDE4MjU4NDM5Njg3NjZdfQ==; _uetsid=50f7c550be2311f0940e9bfe639c74a4; _uetvid=50f7e640be2311f080c395e793823310; _cq_pxg=3|p7540524170993107026644946499; FCCDCF=%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%226d740418-bc8a-4f2a-bd34-bf1c2ecf5a85%5C%22%2C%5B1762771964%2C96000000%5D%5D%22%5D%5D%5D; FCNEC=%5B%5B%22AKsRol-5W8HZTX4B77SYl3I8RPx6vsdRq-o5IStvZsI-6goTReUCK5zkW1N-2I2eJv-UppQNQxOlM9z6oAEQr6WeCCPwNhMiJq06SplepUnGNCzzAfpPVqUGRaDGODvxhnMO2aHzLvt_4OYVUkhJOIxx7FsJO_HsSA%3D%3D%22%5D%5D; last-search-feed=atype%3DC%26custtype%3DP%26cy%3DCA%26damaged_listing%3Dexclude%26desc%3D1%26lat%3D43.029117584228516%26lon%3D-81.26272583007812%26offer%3DU%26size%3D40%26sort%3Dage%26ustate%3DN%252CU%26zip%3DN5X0E2%2520London%252C%2520ON%26zipr%3D1000; _ga_YKMVVRSW3Y=GS2.1.s1762771931$o1$g1$t1762772105$j10$l0$h0; _ga_TX2QRVWP93=GS2.1.s1762771931$o1$g1$t1762772105$j60$l0$h987418783; ___iat_vis=5474ECF60E041E03.d365e08ebc04d931d71f0f6e5c8b9051.1762772106907.6f92ff3b6ad04d7a6960250481c51d7a.ROAIMMMOOZ.11111111.1-0.d365e08ebc04d931d71f0f6e5c8b9051; _clsk=1husmi%5E1762772107776%5E3%5E0%5Eo.clarity.ms%2Fcollect; panoramaId_expiry=1762858506944',
        }

        params = {
            'atype': 'C',
            'custtype': 'P',
            'cy': 'CA',
            'damaged_listing': 'exclude',
            'desc': '1',
            'lat': '43.029117584228516',
            'lon': '-81.26272583007812',
            'offer': 'U',
            'search_id': '1na5hpglm9v',
            'size': '40',
            'sort': 'age',
            'source': 'homepage_search-mask',
            'ustate': 'N,U',
            'zip': 'N5X0E2 London, ON',
            'zipr': '1000',
        }

        response = requests.get('https://www.autotrader.ca/lst', params=params, cookies=cookies, headers=headers, verify=False)
        html = response.text 
        match = re.search(r'<script[^>]+type="application/json"[^>]*>(.*?)</script>', html, re.DOTALL)
        if match:
            json_text = match.group(1).replace('&quot;', '"')
            data = json.loads(json_text)
            # print(json.dumps(data, indent=2))
            cars = data['props']['pageProps']['listings']
            
            for car in cars:
                make = car["vehicle"].get("make", "")
                model = car["vehicle"].get("model", "")
                year = car["vehicle"].get("modelYear", "")
                price = car["price"].get("priceFormatted", "")
                mileage = car["vehicle"].get("mileageInKm", "")
                city = car["location"].get("city", "")
                url = car.get("url", "")
                description = car.get("description", "").split("<br")[0]  # short preview
                image = car["images"][0] if car.get("images") else "N/A"
                insert_car_autotreader(f"{year} {make} {model}", price, city, mileage, image, url)
                

                print(f"{year} {make} {model}")
                print(f"  Price: {price}")
                print(f"  Mileage: {mileage}")
                print(f"  City: {city}")
                print(f"  Image: {image}")
                print(f"  URL: {url}")
                print(f"  Description: {description}\n")
        else:
            st.warning("No embedded JSON found.")

        st.success("✅ Done! Autotrader Cars successfully added to the database.")

            # cars.append(car)

        # print(json.dumps(cars, indent=4, ensure_ascii=False))

