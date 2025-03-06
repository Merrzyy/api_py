from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
from datetime import datetime

# --- Load environment variables from .env ---
load_dotenv()

# --- Database Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")
#the database url is now in the .env

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set.")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Database Model ---
class KpopIdol(Base):
    __tablename__ = "kpop_idol_followers"


    Stage_Name = Column(String(255), primary_key=True, index=True)  # Make Stage_Name the primary key
    Group = Column(String(255), index=True)
    ig_name = Column(String(255))
    Followers = Column(Integer)
    Gender_x = Column(String(255))
    Full_Name = Column(String(255))
    Korean_Name = Column(String(255))
    K_Stage_Name = Column(String(255))
    Date_of_Birth = Column(DateTime)
    Debut = Column(DateTime)
    Company = Column(String(255), index=True)
    Country = Column(String(255), index=True)
    Second_Country = Column(String(255))
    Height = Column(String(255), nullable=True)  # Allow None
    Weight = Column(String(255), nullable=True)  # Allow None
    Birthplace = Column(String(255))
    Other_Group = Column(String(255))
    Former_Group = Column(String(255))
    Gender_y = Column(String(255))
    age = Column(Integer)
    year_career = Column(Integer)

# --- FastAPI App ---
app = FastAPI(
    title="K-Pop Idol API (TiDB)",
    description="API for K-Pop Idol information (using TiDB).",
    version="1.0.0",
)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- API Endpoints ---

@app.get("/", response_model=List[Dict], summary="Get All K-Pop Idols")
async def get_all_idols(db: Session = Depends(get_db)):
    """
    Retrieves all K-Pop idol information from the database.
    """
    idols = db.query(KpopIdol).all()
    return [ {k: v for k, v in idol.__dict__.items() if not k.startswith('_')} for idol in idols]

@app.get("/idol/{stage_name}", response_model=List[Dict], summary="Get K-Pop Idol by Stage Name")
async def get_idol_by_stage_name(stage_name: str, db: Session = Depends(get_db)):
    """
    Retrieves a specific K-Pop idol's information by their stage name from the database.

    Args:
        stage_name: The stage name of the K-Pop idol.
    """
    idol = db.query(KpopIdol).filter(KpopIdol.Stage_Name == stage_name).all()
    if not idol:
        raise HTTPException(status_code=404, detail="Idol not found.")
    return [ {k: v for k, v in i.__dict__.items() if not k.startswith('_')} for i in idol]


@app.get("/group/{group_name}", response_model=List[Dict], summary="Get K-Pop Idols by Group")
async def get_idols_by_group(group_name: str, db: Session = Depends(get_db)):
    """
    Retrieves a list of K-Pop idols belonging to a specific group from the database.

    Args:
        group_name: The name of the K-Pop group.
    """
    idols = db.query(KpopIdol).filter(KpopIdol.Group == group_name).all()
    if not idols:
        raise HTTPException(status_code=404, detail="Group not found or has no members in the dataset.")
    return [ {k: v for k, v in idol.__dict__.items() if not k.startswith('_')} for idol in idols]

@app.get("/search/", response_model=List[Dict], summary="Search K-Pop Idols by any field")
async def search_idols(field: str = None, value: str = None, db: Session = Depends(get_db)):
    """
    Search for K-Pop idols based on any field and value.

    Args:
        field: The field to search (e.g., 'Group', 'Gender_x', 'Country').
        value: The value to search for.
    """
    if not field or not value:
        raise HTTPException(status_code=400, detail="Both 'field' and 'value' are required.")

    if field not in KpopIdol.__table__.columns:
        raise HTTPException(status_code=400, detail=f"Field '{field}' not found in the data.")

    # Dynamic query
    query_str = f"SELECT * FROM kpop_idol_followers WHERE `{field}` LIKE '%{value}%'"
    
    if field in ["Height","Weight"] : # handle NA
        query_str = f"SELECT * FROM kpop_idol_followers WHERE `{field}` LIKE '%{value}%' OR `{field}` IS NULL"
    
    result = db.execute(text(query_str)).mappings().all()

    if not result:
        raise HTTPException(status_code=404, detail="No idols found matching the search criteria.")
    return [dict(row) for row in result]

@app.get("/filter/", response_model=List[Dict], summary="Filter K-Pop Idols by Various Criteria")
async def filter_idols(
    gender: Optional[str] = None,
    country: Optional[str] = None,
    company: Optional[str] = None,
    debut_year: Optional[int] = None,
    age_from: Optional[int] = None,
    age_to: Optional[int] = None,
    db: Session = Depends(get_db),
):
    """
    Filter K-Pop idols based on multiple criteria.

    Args:
        gender: Filter by gender ('Boy' or 'Girl').
        country: Filter by country.
        company: Filter by company.
        debut_year: Filter by debut year.
        age_from: Filter by age from.
        age_to: Filter by age to.
    """
    query_str = "SELECT * FROM kpop_idol_followers WHERE 1=1"
    params = {}

    if gender:
      query_str += " AND `Gender_x` LIKE :gender"
      params["gender"] = f"%{gender}%"

    if country:
      query_str += " AND `Country` LIKE :country"
      params["country"] = f"%{country}%"

    if company:
      query_str += " AND `Company` LIKE :company"
      params["company"] = f"%{company}%"

    if debut_year:
        query_str += " AND YEAR(`Debut`) = :debut_year"
        params["debut_year"] = debut_year

    if age_from:
        query_str += " AND `age` >= :age_from"
        params["age_from"] = age_from

    if age_to:
        query_str += " AND `age` <= :age_to"
        params["age_to"] = age_to
        
    if any(field in ["Height", "Weight"] for field in params.keys()):
       query_str += " OR (`Height` IS NULL OR `Weight` IS NULL)"

    result = db.execute(text(query_str), params).mappings().all()
    
    if not result:
      raise HTTPException(status_code=404, detail="No idols found matching the filter criteria.")
    return [dict(row) for row in result]
