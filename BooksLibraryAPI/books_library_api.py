from datetime import datetime, timedelta, date
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from jose import jwt, JWTError
import uvicorn
from books_library_db import LibraryDB 
import os

app = FastAPI()

library_db = LibraryDB()

# openssl rand -hex 32
SECRET_KEY = os.environ['SECRET_KEY']
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if library_db.user_exist(username) is False:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username


class Book(BaseModel):
    title: str
    author: str
    published_date: date|None
    quantity: int|None

class User(BaseModel):
    user_name: str
    name: str|None
    email: str|None
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

@app.post("/sign-up", tags=['Users'])
def sign_up(user_details: User):
    return library_db.add_user(user_details.user_name, user_details.name, user_details.email, user_details.password)

@app.post("/login", response_model=Token, tags=['Users'])
def login(formdata: OAuth2PasswordRequestForm = Depends()):
    if library_db.verify_user(formdata.username, formdata.password):
        access_token = create_access_token({"sub": formdata.username})
        return {"access_token": access_token, "token_type": "bearer"}
    raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

@app.get("/", tags=['Library books'])
def retrieve_all_books(current_user: str = Depends(get_current_user)):
    return f"Books in our library: {library_db.get_all_books_from_db()}"

@app.get("/book/", tags=['Library books'])
def retrieve_a_book(book_title: str, current_user: str = Depends(get_current_user)):
    return library_db.get_book_from_db(book_title)

@app.post("/add-book/", status_code=status.HTTP_201_CREATED, tags=['Library books'])
def add_book_to_library(book_to_add: Book, current_user: str = Depends(get_current_user)):
    return library_db.add_book_to_db(book_to_add.title, book_to_add.author, book_to_add.published_date, book_to_add.quantity)

@app.put("/update-book/", tags=['Library books'])
def update_book_info(book_title: str, new_book_info: Book, current_user: str = Depends(get_current_user)):
    return library_db.update_book_in_db(book_title, new_book_info.title, new_book_info.author, new_book_info.published_date, new_book_info.quantity)

@app.delete("/delete-book/", tags=['Library books'])
def delete_book(book_title: str, current_user: str = Depends(get_current_user)):
    return library_db.delete_book_from_db(book_title)

if __name__ == '__main__':
    uvicorn.run("books_library_api:app", port=8000, reload=True)