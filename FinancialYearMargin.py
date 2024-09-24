import requests  # Library for making HTTP requests
import gspread  # Library for interacting with Google Sheets
from oauth2client.service_account import ServiceAccountCredentials  # For Google Sheets API authentication
import json  # Library for handling JSON data
from datetime import datetime, timedelta  # For date manipulation
import currencyapicom  # For fetching currency exchange rates

# Load credentials from the JSON file for Google Sheets API access
scope = [
    'https://spreadsheets.google.com/feeds',  # Scope for Google Sheets
    'https://www.googleapis.com/auth/drive'    # Scope for Google Drive
]
credentials = ServiceAccountCredentials.from_json_keyfile_name('<serverkey json file>', scope)

# Authenticate and create a client to interact with Google Sheets API
gc = gspread.authorize(credentials)

# Open the specified spreadsheet by its URL
sheet = gc.open_by_url('<yourGoogleSheet>')

# Select the worksheet by name
worksheet = sheet.worksheet('CurrentMargin')  # Change 'CurrentMargin' to your actual sheet name if needed

class HubSpotAPI:
    def __init__(self):
        # Initialize API access details for HubSpot
        self.access_token = "<your API Token>"  # HubSpot API token
        self.url = "https://api.hubspot.com/crm/v3/objects/deals/search"  # URL for HubSpot deals API
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",  # Set the authorization header
            "Content-Type": "application/json"  # Specify content type as JSON
        }
        
    def find_deals(self, start_date, end_date, deal_stage, page):
        """
        Fetch deals from HubSpot based on date range and deal stages.

        Args:
            start_date (str): The start date for the deals.
            end_date (str): The end date for the deals.
            deal_stage (list): List of deal stages to filter.
            page (str): Pagination type ("limit" or "after").

        Returns:
            list: A list of deals retrieved from HubSpot.
        """
        # Prepare the request data for fetching deals
        data = {
            "limit": 100,  # Limit the number of results to 100
            page: 100,  # Use the provided pagination type
            "properties": ["net_revenue", "amount", "dealstage", "deal_currency_code"],  # Properties to retrieve
            "filterGroups": [
                {
                    "filters": [
                        {"propertyName": "closedate", "operator": "BETWEEN", "highValue": end_date, "value": start_date},  # Filter by date range
                        {"propertyName": "dealstage", "operator": "IN", "values": deal_stage}  # Filter by deal stages
                    ]
                }
            ]
        }

        # Make a POST request to the HubSpot API to fetch deals
        response = requests.post(self.url, headers=self.headers, json=data)
        if response.status_code == 200:
            # If successful, return the list of deals
            return response.json().get('results', [])
        else:
            # Print error details if the request fails
            print("Request failed:", response.status_code, response.text)
            return []

    def extract_data(self, deals, rate):
        """
        Extract and calculate financial metrics from the fetched deals.

        Args:
            deals (list): The list of deals retrieved from HubSpot.
            rate (float): The exchange rate from NZD to AUD.

        Returns:
            tuple: Calculated metrics including margin and total deal count.
        """
        # Initialize variables for aggregating data
        idcal = []  # List to store deal IDs
        num_amounts = 0  # Total amounts counter
        num_net_revenue = 0  # Total net revenue counter

        # Loop through each deal to extract relevant information
        for deal in deals:
            amount = deal['properties'].get('amount')  # Get the deal amount
            id = deal['properties'].get('hs_object_id')  # Get the deal ID
            net_revenue = deal['properties'].get('net_revenue')  # Get the net revenue
            currency = deal['properties'].get('deal_currency_code')  # Get the currency type

            idcal.append(id)  # Store the deal ID for reference

            # If the currency is NZD, convert the net revenue to AUD
            if currency == "NZD":
                if rate is not None:  # Ensure the exchange rate was successfully fetched
                    net_revenue_aud = float(net_revenue) / rate  # Convert net revenue to AUD
                    net_revenue = net_revenue_aud  # Update net revenue to AUD

            # Aggregate the total amounts and net revenue
            num_amounts += float(amount)  # Aggregate the deal amounts
            if net_revenue:
                num_net_revenue += float(net_revenue)  # Aggregate net revenue if available
            else:
                num_net_revenue += float(amount)  # Use the amount if net revenue is not available

            # Calculate margin based on aggregated data
            margin = ((num_net_revenue / num_amounts) * 100)  # Calculate profit margin

        return margin, len(idcal)  # Return the calculated margin and the number of deals

    def get_nz_to_aud_rate(self):
        """
        Fetch the latest NZD to AUD exchange rate.

        Returns:
            float: The conversion rate from NZD to AUD, or None if an error occurs.
        """
        client = currencyapicom.Client('<currency API>')  # Initialize the currency client
        try:
            # Fetch the latest exchange rate for NZD to AUD
            result = client.latest('AUD', currencies=['NZD'])
            amount = result['data']['NZD']['value']  # Get the exchange rate value
            amount = float(amount)  # Convert the rate to a float

            return amount  # Return the exchange rate
        except Exception as e:
            # Print error message if fetching the exchange rate fails
            print(f"Error fetching exchange rate: {e}")
            return None

    def alltime(self, deal_stage, quarterstart, quarterend, financialstart, financialend, rate):
        """
        Fetch and compute margin and total deal counts for specified quarters and financial years.

        Args:
            deal_stage (list): List of deal stages to filter.
            quarterstart (str): Start date of the current quarter.
            quarterend (str): End date of the current quarter.
            financialstart (str): Start date of the financial year.
            financialend (str): End date of the financial year.
            rate (float): The exchange rate from NZD to AUD.

        Returns:
            tuple: Lists of margins and total deal counts for each period.
        """
        # Initialize lists to store margins and totals
        margins = []
        totals = []

        # Define quarters and their respective start and end dates
        quarters = [
            ("Q1", quarterstart, quarterend),
            ("Financial Year", financialstart, financialend)
        ]

        # Loop through each quarter and deal stage category to fetch deals
        for quarter, start, end in quarters:
            for stage_category in deal_stage:
                deals = self.find_deals(start, end, stage_category, "limit")  # Fetch deals with "limit"
                deals_after = self.find_deals(start, end, stage_category, "after")  # Fetch deals with "after" pagination
                all_deals = deals + deals_after  # Combine both results

                if all_deals:  # Check if any deals were retrieved
                    # Extract and compute data
                    margin, total = self.extract_data(all_deals, rate)
                    margins.append(margin)  # Append margin to list
                    totals.append(total)  # Append total deal count to list
                else:
                    margins.append("NA")  # Append "NA" if no deals were found
                    totals.append("NA")
                    print(f"No deals found for {quarter}.")  # Log message if no deals were found

        return margins, totals  # Return the computed margins and totals

    def get_current_quarter_dates(self):
        """
        Get the start and end dates of the current quarter.

        Returns:
            tuple: Start and end dates of the current quarter in YYYY-MM-DD format.
        """
        today = datetime.today()  # Get today's date

        # Determine the start date of the current quarter
        quarter_start = (today.month - 1) // 3 * 3 + 1  # Calculate quarter start month
        start_date = datetime(today.year, quarter_start, 1)  # Start date of the quarter

        # Calculate the last day of the current quarter
        next_quarter_start = datetime(today.year, quarter_start + 3, 1) if quarter_start < 10 else datetime(today.year + 1, 1, 1)
        end_date = next_quarter_start - timedelta(days=1)  # Last day of the current quarter

        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')  # Return formatted dates

    def get_current_financial_year_dates(self):
        """
        Get the start and end dates of the current financial year.

        Returns:
            tuple: Start and end dates of the current financial year in YYYY-MM-DD format.
        """
        today = datetime.today()  # Get today's date
        start_date = datetime(today.year, 7, 1) if today.month >= 7 else datetime(today.year - 1, 7, 1)  # Determine start date
        end_date = datetime(today.year + 1, 6, 30) if today.month >= 7 else datetime(today.year, 6, 30)  # Determine end date
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')  # Return formatted dates

def main():
    """
    Main function to execute the report generation process.
    Fetches data from HubSpot, processes it, and writes results to Google Sheets.
    """
    all = ["123633772", "848f19bf-930a-4f0f-bbc5-8d4b69d2cc3a"]  # List of all deal stages
    sales = ["123633772"]  # List of sales deal stages
    renewels = ["848f19bf-930a-4f0f-bbc5-8d4b69d2cc3a"]  # List of renewal deal stages

    hubspot_api = HubSpotAPI()  # Initialize the HubSpot API client
    rate = hubspot_api.get_nz_to_aud_rate()  # Fetch the NZD to AUD conversion rate

    # Get the current quarter and financial year dates
    current_quarter_start, current_quarter_end = hubspot_api.get_current_quarter_dates()
    current_financial_year_start, current_financial_year_end = hubspot_api.get_current_financial_year_dates()

    row = 2  # Starting row for writing data in the worksheet
    col = 1  # Starting column for writing data in the worksheet
    values_entered = 0  # Counter for values entered

    # Fetch margins and totals for the specified deal stages and periods
    margins, totals = hubspot_api.alltime([all, sales, renewels], current_quarter_start, current_quarter_end, current_financial_year_start, current_financial_year_end, rate)
    
    # Loop through the margins and totals to write them to the Google Sheet
    for margin, total in zip(margins, totals):
        excel_col = chr(65 + col)  # Convert column index to Excel column letter
        if margin != "NA":  # Check if margin is valid
            report = f"{round(margin, 2)}% | deals: {total}"  # Format the report string
        else:
            report = "NA"  # If no data, mark as NA
        
        worksheet.update_acell(f'{excel_col}{row}', report)  # Update the cell with the report
        values_entered += 1  # Increment the values entered counter

        # Change column after every 3 values entered
        if values_entered % 3 == 0:
            col += 1  # Move to the next column
            row = 1  # Reset row for the new column
        row += 1  # Move to the next row

if __name__ == "__main__":
    main()  # Execute the main function when the script is run
