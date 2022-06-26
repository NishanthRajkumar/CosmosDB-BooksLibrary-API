import pymongo
import os
from datetime import date

class DBException(Exception):
    def __init__(self, msg) -> None:
        super().__init__(msg)

class LibraryDB():

    def __init__(self) -> None:
        uri = os.environ["CFP_COSMOS_URI"]
        self.client = pymongo.MongoClient(uri)
        if "books_library" not in self.client.list_database_names():
            self.db = self.client["books_library"]
            self.db.create_collection(name="books")
            self.db.create_collection(name="users")
        else:
            self.db = self.client["books_library"]
        self.books = self.db["books"]
        self.users = self.db["users"]

    def get_all_books_from_db(self) -> dict|str:
        """
            Description:
                Retrieves all books from the Database
            
            Return:
                returns a Dictionary
        """
        try:
            books_list = []
            for book in self.books.find():
                books_list.append(book)
            return books_list
        except Exception as e:
            return f"Failed to execute query: {e}"
    
    def get_book_from_db(self, title: str) -> dict|str:
        """
            Description:
                Retrieves a book from the Database that matches title
            
            Parameter:
                title: Title of the book
            
            Return:
                returns a Dictionary
        """
        try:
            book = self.books.find_one({"title": title})
            return book
        except Exception as e:
            return f"Failed to execute query: {e}"
    
    def add_book_to_db(self, title: str, author: str, pub_date: date, qty: int) -> str:
        """
            Description:
                Adds a book to the Database
            
            Parameter:
                title: Title of the book
                author: Author of the book
                pub_date: Published date
                qty: quantity
            
            Return:
                returns a string
        """
        book = {
            "_id": title,
            "title": title,
            "author": author,
            "published_date": pub_date.strftime("%d/%m/%Y"),
            "quantity": qty
        }
        try:
            self.books.insert_one(book)
        except Exception as e:
            return f"Failed to execute query: {e}"
        else:
            return "Succesfully added the book to library"
    
    def update_book_in_db(self, old_title: str,new_title: str, author: str, pub_date: date, qty: int) -> str:
        """
            Description:
                Updates a book in the Database
            
            Parameter:
                old_title: Title of the book to edit
                new_title: Updated title of the book
                author: Updated Author of the book
                pub_date: Updated Published date
                qty: quantity
            
            Return:
                returns a string
        """
        upd_book = {
            "_id": new_title,
            "title": new_title,
            "author": author,
            "published_date": pub_date.strftime("%d/%m/%Y"),
            "quantity": qty
        }
        try:
            match_query = {"title": old_title}
            updated_val = {"$set": upd_book}
            self.books.update_one(match_query, updated_val)
        except Exception as e:
            return f"Failed to execute query: '{new_title}' already exists in books collection"
        else:
            return "Succesfully updated the book"
    
    def delete_book_from_db(self, title: str) -> str:
        """
            Description:
                Deletes a book from the Database that matches title
            
            Parameter:
                title: Title of the book
            
            Return:
                returns a string
        """
        try:
            match_query = {"_id": title}
            if self.books.find_one(match_query) is None:
                return f"'{title}' does not exist in library"
            self.books.delete_one(match_query)
        except Exception as e:
            return f"Failed to execute query: {e}"
        else:
            return "Succesfully deleted the book"
    
    def add_user(self, user_name: str, name: str, email: str, password: str):
        """
            Description:
                Adds user to user collection
            
            Parameter:
                user_name: A unique string for user name. Used for login.
                name: Not unique. Name of user(Optional)
                password: password as string
            
            Return:
                returns a string
        """
        user = {
            "_id": user_name,
            "user_name": user_name,
            "name": name,
            "email": email,
            "password": password
        }
        try:
            match_query = {"email": email}
            if self.users.find_one(match_query) is not None:
                return f"'{email}' already exists in users collection"
            self.users.insert_one(user)
        except Exception as e:
            return f"Failed to execute query: user name '{user_name}' already exists"
        else:
            return "Succesfully added user"
    
    def verify_user(self, user_name: str, password: str) -> bool:
        """
            Description:
                Adds user to user collection
            
            Parameter:
                user_name: A unique string for user name. Used for login.
                password: password as string
            
            Return:
                returns bool
        """
        try:
            match_query = {"user_name": user_name, "password": password}
            if self.users.find_one(match_query) is None:
                return False
            return True
        except Exception as e:
            print(f"Failed to execute query: {e}")
    
    def user_exist(self, user_name) -> bool:
        """
            Description:
                Check if user is in user collection
            
            Parameter:
                user_name: A unique string for user name. Used for login.
            
            Return:
                returns bool
        """
        try:
            match_query = {"user_name": user_name}
            if self.users.find_one(match_query) is None:
                return False
            return True
        except Exception as e:
            print(f"Failed to execute query: {e}")