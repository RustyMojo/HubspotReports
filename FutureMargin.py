import requests  # Library to make HTTP requests
import gspread  # Library to interact with Google Sheets
from oauth2client.service_account import ServiceAccountCredentials  # For Google Sheets authentication
import json  # Library for handling JSON data
from datetime import date  # To work with date objects
import currencyapicom  # For fetching currency exchange rates

# Load credentials from the JSON file for Google Sheets API access
scope = [
    'https://spreadsheets.google.com/feeds',  # Scope for Google Sheets API
    'https://www.googleapis.com/auth/drive'    # Scope for Google Drive API
]

# Authenticate using the service account credentials
credentials = ServiceAccountCredentials.from_json_keyfile_name('<serverkey json file>', scope)

# Authorize and create a client to interact with the Google Sheets API
gc = gspread.authorize(credentials)

# Open the specified Google Spreadsheet by its URL
sheet = gc.open_by_url('<googlesheetsBeingUsed>')

# Select the specific worksheet by its name
worksheet = sheet.worksheet('AllTime')  # Replace with the actual sheet name you want to use

class HubSpotAPI:
    def __init__(self):
        # Initialize API access details for HubSpot
        self.access_token = <insert your token>  # HubSpot API token
        self.url = "https://api.hubspot.com/crm/v3/objects/deals/search"  # URL for HubSpot deals API
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",  # Set authorization header
            "Content-Type": "application/json"  # Specify content type as JSON
        }
        
    def find_deals(self, page, length):
        """
        Fetch deals from HubSpot based on pagination and filters.

        Args:
            page (str): The pagination type (e.g., "limit" or "after").
            length (int): The length or offset for pagination.

        Returns:
            list: A list of deals retrieved from HubSpot.
        """
        # Prepare the request data for fetching deals
        data = {
            "limit": 100,  # Limit the number of results to 100
            page: length,  # Use the provided page parameter
            "properties": ["net_revenue", "amount_in_home_currency", "dealstage", "deal_currency_code"],
            "filterGroups": [
                {
                    "filters": [
                        {"propertyName": "pipeline", "operator": "EQ", "value": "af6b8780-5d77-490a-83d8-883156928208"},
                        {"propertyName": "dealstage", "operator": "NOT_IN", "values": [
                            "848f19bf-930a-4f0f-bbc5-8d4b69d2cc3a",
                            "faf489c8-9e2d-4120-ae7c-bafe9b2d643d",
                            "0f439617-c80e-4f12-b90b-66e9c861aec5"
                        ]}
                    ]
                }
            ]
        }

        # Make a POST request to the HubSpot API to fetch deals
        response = requests.post(self.url, headers=self.headers, json=data)
        if response.status_code == 200:
            # If successful, return the list of deals from the response
            return response.json().get('results', [])
        else:
            # Print error details if the request fails
            print("Request failed:", response.status_code, response.text)
            return []
        
    def get_nz_to_aud_rate(self):
        """
        Fetch the latest NZD to AUD exchange rate.

        Returns:
            float: The conversion rate from NZD to AUD, or None if an error occurs.
        """
        # Initialize the currency client to fetch exchange rates
        client = currencyapicom.Client('<api for currentapi.com>')
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
    
    def extract_data(self, deals, rate):
        """
        Extract and aggregate financial metrics from the fetched deals.

        Args:
            deals (list): The list of deals retrieved from HubSpot.
            rate (float): The exchange rate from NZD to AUD.

        Returns:
            tuple: Calculated metrics including margin, deal count, total amount, net revenue, and cost.
        """
        # Initialize variables for aggregating data
        idcal = []  # List to store deal IDs
        num_amounts = 0  # Total amounts counter
        num_net_revenue = 0  # Total net revenue counter
        
        # Loop through each deal to extract relevant information
        for deal in deals:
            amount = deal['properties'].get('amount_in_home_currency')  # Get the deal amount
            id = deal['properties'].get('hs_object_id')  # Get the deal ID
            net_revenue = deal['properties'].get('net_revenue')  # Get the net revenue
            currency = deal['properties'].get('deal_currency_code')  # Get the currency type
            idcal.append(id)  # Store the deal ID for reference

            # If the currency is NZD, convert the net revenue to AUD
            if currency == "NZD":
                if rate is not None:  # Ensure the exchange rate was successfully fetched
                    net_revenue_aud = float(net_revenue) / rate  # Convert net revenue to AUD
                    net_revenue = net_revenue_aud  # Update the net revenue to AUD

            num_amounts += float(amount)  # Aggregate the total amounts
            if net_revenue:
                num_net_revenue += float(net_revenue)  # Aggregate net revenue if available
            else:
                num_net_revenue += float(amount)  # Use amount if net revenue is not available

            # Calculate margin and cost
            margin = ((num_net_revenue / num_amounts) * 100)  # Calculate profit margin
            cost = num_amounts - num_net_revenue  # Calculate cost

        # Return calculated metrics as a tuple
        return margin, len(idcal), num_amounts, num_net_revenue, cost

    def increment_file_number(self):
        """
        Increment a number stored in a text file and return the previous number.

        Returns:
            int: The previous number before incrementing.
        """
        filename = "rownumber.txt"  # Name of the file storing the row number
        try:
            # Open the file in read mode to get the current number
            with open(filename, "r") as file:
                number_str = file.read()  # Read the number as a string
                number = int(number_str)  # Convert the string to an integer

            previousnumber = number  # Store the previous number before incrementing
            number += 1  # Increment the number by 1

            # Open the file again in write mode to save the incremented number
            with open(filename, "w") as file:
                file.write(str(number))  # Write the new number back to the file
                return previousnumber  # Return the previous number
        except IOError as e:
            # Raise an error if there was an issue accessing the file
            raise IOError(f"An error occurred while accessing the file: {e}") from e
  
    def get_next_open_row(self, worksheet):
        """
        Find the next available row in the specified worksheet.

        Args:
            worksheet: The Google Sheets worksheet object.

        Returns:
            int: The next available row number, or None if no rows are available.
        """
        currentrow = self.increment_file_number()  # Get the current row from the file
        print("Current row:", currentrow)

        # Check for the next empty row starting from currentrow
        for row in range(currentrow, worksheet.row_count + 1):  
            if worksheet.cell(row=row, col=1).value is None:  # Check if the first cell in the row is empty
                print("Next available row:", row)  # Print the found empty row
                return row  # Return the found empty row number
        return None  # Return None if no empty rows are found

    def write_data_to_sheet(self, data, row, worksheet):
        """
        Write a list of data to the specified row in the Google Sheet.

        Args:
            data (list): List of values to write to the sheet.
            row (int): The row number where data should be written.
            worksheet: The Google Sheets worksheet object.
        """
        col = 0  # Initialize column index
        for value in data:
            excel_col = chr(65 + col)  # Convert column index to letter (A=0, B=1, ...)
            worksheet.update_acell(f'{excel_col}{row}', value)  # Update the cell with the value
            col += 1  # Increment column index for the next value

def main():
    """
    Main function to execute the report generation process.
    It fetches deals, processes data, and writes results to Google Sheets.
    """
    today = date.today()  # Get today's date for reporting
    today = str(today)  # Convert date to string format

    # Initialize the HubSpot API client
    hubspot_api = HubSpotAPI()
    
    # Fetch the NZD to AUD conversion rate
    rate = hubspot_api.get_nz_to_aud_rate()
    
    # Retrieve deals from HubSpot API
    answer = hubspot_api.find_deals("limit", 100)
    length = len(answer)  # Get the length of the initial response

    # Continue fetching deals if the last batch had 100 deals (indicating more available)
    while length % 100 == 0:
        answer += hubspot_api.find_deals("after", length)  # Fetch additional deals
        length = len(answer)  # Update length after fetching

    # Extract aggregated financial data from the deals
    margin, dealcount, amount, netRevenue, cost = hubspot_api.extract_data(answer, rate)
    
    # Round calculated values for more readable reporting
    margin = round(margin, 2)  # Round margin to 2 decimal places
    margin = f"{margin}%"  # Format margin as a percentage
    amount = round(amount, 2)  # Round total amount
    netRevenue = round(netRevenue, 2)  # Round net revenue
    cost = round(cost, 2)  # Round cost

    # Find the next open row in the worksheet to write data
    next_row = hubspot_api.get_next_open_row(worksheet)
    if next_row is None:
        print("Google Sheet is full!")  # Handle case where the sheet is full
        return  # Exit if no rows are available

    # Prepare data to write to the Google Sheet
    data_to_write = [today, margin, dealcount, amount, netRevenue, cost]  # Collect data in a list

    # Write the prepared data to the Google Sheet
    hubspot_api.write_data_to_sheet(data_to_write, next_row, worksheet)

    # Confirm that the report was written successfully
    print(f"Daily report for {today} written to your_workbook")

if __name__ == "__main__":
    main()  # Execute the main function when the script is run
