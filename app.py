# app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables at startup
load_dotenv()

# Get API key from environment
FMCSA_API_KEY = os.environ.get('FMCSA_API_KEY')
if not FMCSA_API_KEY:
    raise ValueError("FMCSA_API_KEY must be set in environment variables")

# Constants
FMCSA_BASE_URL = "https://mobile.fmcsa.dot.gov/qc/services/carriers/"

# Models for carrier validation
class CarrierResponse(BaseModel):
    success: bool
    data: Dict[str, Any]

# Models for load details
class LoadDetails(BaseModel):
    reference_number: str
    origin: str
    destination: str
    equipment_type: str
    rate: float
    commodity: str

class LoadResponse(BaseModel):
    success: bool
    data: Optional[LoadDetails] = None
    error: Optional[str] = None

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def validate_mc_number(mc_number: str) -> CarrierResponse:
    """Validate MC number using the FMCSA API"""
    if not FMCSA_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="FMCSA API key not configured"
        )

    # Validate input format
    if not mc_number:
        raise HTTPException(
            status_code=400,
            detail="MC number is required"
        )

    clean_mc = mc_number.upper().replace('MC-', '').strip()
    
    # Validate MC number format
    if not clean_mc.isdigit():
        raise HTTPException(
            status_code=400,
            detail="Invalid MC number format. Must contain only digits after removing 'MC-' prefix"
        )
    
    try:
        url = f"{FMCSA_BASE_URL}{clean_mc}?webKey={FMCSA_API_KEY}"
        
        try:
            response = requests.get(url, timeout=10)  # Add timeout
        except requests.exceptions.Timeout:
            raise HTTPException(
                status_code=504,
                detail="FMCSA API request timed out"
            )
        except requests.exceptions.RequestException as e:
            raise HTTPException(
                status_code=502,
                detail=f"Error connecting to FMCSA API: {str(e)}"
            )
        
        # Handle different HTTP status codes
        if response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"Carrier with MC number {clean_mc} not found"
            )
        elif response.status_code == 401:
            raise HTTPException(
                status_code=502,
                detail="Invalid FMCSA API key"
            )
        elif response.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"FMCSA API error: {response.status_code}"
            )
        
        try:
            data = response.json()
        except ValueError:
            raise HTTPException(
                status_code=502,
                detail="Invalid JSON response from FMCSA API"
            )
        if not data.get('content', {}):
            raise HTTPException(
                status_code=404,
                detail=f"No carrier data found for MC number: {clean_mc}"
            )  
        # Extract carrier information from the correct response structure
        carrier_data = data.get('content', {}).get('carrier', {})
        

        
        # Validate required fields
        carrier_name = carrier_data.get('legalName', carrier_data.get('dbaName', ''))
        dot_number = carrier_data.get('dotNumber')
        
        if not carrier_name or not dot_number:
            raise HTTPException(
                status_code=502,
                detail="Incomplete carrier data received from FMCSA API"
            )
        
        # Determine if carrier is authorized to operate
        is_authorized = carrier_data.get('allowedToOperate') == 'Y'
        status = "Active" if is_authorized else "Inactive"
        
        # Get additional status details
        status_reason = None
        if carrier_data.get('oosDate'):
            status_reason = "Out of Service"
        elif not is_authorized:
            status_reason = "Not Authorized to Operate"

        return CarrierResponse(
            success=True,
            data={
                "carrier": {
                    "carrier_id": str(dot_number),
                    "status": status,
                    "carrier_name": carrier_name,
                    "dot_number": str(dot_number),
                    "mc_number": clean_mc,
                    "status_reason": status_reason
                },
                "transfer_contact": None,
                "next_steps": {
                    "1": f"Confirm you found the right carrier name. Ask user exactly this: '{carrier_name}?'. Then wait for user to respond.",
                    "2": {
                        "a": f"If user confirms: move on to finding available loads, MAKE SURE you do not give them load information until you have verified they work for the carrier {carrier_name}.",
                        "b": "It is possible that you may have transcribed the name incorrectly, so use your best judgement to decide if the name the caller gives is close enough to the carrier name you have. If so, you can consider it a match and move on to finding available loads.",
                        "c": "If user denies: first, you must ask the user to repeat the name of the carrier they work for ('I'm sorry, I didn't quite catch that. What's the name of the carrier you work for?'). Wait for the user to provide the name again and check if it matches the carrier name you have.",
                        "d": "If you still cannot verify the carrier name, ask the user for their MC / DOT number ('I'm sorry, what's that MC number again?'). Wait for user to provide number again. Then search for the carrier again with new number the caller provides."
                    }
                }
            }
        )
            
    except HTTPException:
        raise
    except Exception as e:
        # Log unexpected errors and return 500
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while processing your request"
        )

def get_load_details(reference_number: str) -> LoadResponse:
    """
    Retrieve load details from CSV file based on reference number
    """
    try:
        # Assuming the CSV file is in the same directory as the script
        csv_path = Path(__file__).parent / "loads.csv"
        
        if not csv_path.exists():
            raise HTTPException(
                status_code=503,
                detail="Load data file not available"
            )
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            raise HTTPException(
                status_code=503,
                detail="Load data file is empty"
            )
        except pd.errors.ParserError:
            raise HTTPException(
                status_code=503,
                detail="Error parsing load data file"
            )
        
        # Validate reference number format
        if not reference_number.startswith('LOAD'):
            raise HTTPException(
                status_code=400,
                detail="Invalid reference number format. Must start with 'LOAD'"
            )
        
        # Find the load with matching reference number
        load = df[df['reference_number'] == reference_number]
        
        if load.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Load not found: {reference_number}"
            )
        
        try:
            # Convert the first (and should be only) row to a dictionary
            load_data = load.iloc[0].to_dict()
            
            # Validate required fields
            required_fields = ['reference_number', 'origin', 'destination', 'equipment_type', 'rate', 'commodity']
            missing_fields = [field for field in required_fields if field not in load_data or pd.isna(load_data[field])]
            
            if missing_fields:
                raise HTTPException(
                    status_code=500,
                    detail=f"Missing required fields in load data: {', '.join(missing_fields)}"
                )
            
            # Create LoadDetails object with validation
            load_details = LoadDetails(
                reference_number=str(load_data['reference_number']),
                origin=str(load_data['origin']),
                destination=str(load_data['destination']),
                equipment_type=str(load_data['equipment_type']),
                rate=float(load_data['rate']),
                commodity=str(load_data['commodity'])
            )
            
            return LoadResponse(
                success=True,
                data=load_details
            )
            
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error processing load data: {str(e)}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        # Log unexpected errors and return 500
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while processing your request"
        )

@app.get("/api/v1/carriers/validate/{mc_number}", response_model=CarrierResponse)
async def validate_carrier(mc_number: str):
    """Validate a carrier's MC number"""
    return validate_mc_number(mc_number)

@app.get("/api/v1/loads/{reference_number}", response_model=LoadResponse)
async def get_load(reference_number: str):
    """Retrieve load details by reference number"""
    return get_load_details(reference_number)