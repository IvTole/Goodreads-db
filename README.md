# Goodreads-db

A SQLite database project for Goodreads book data analysis.

## Project Overview

This project creates and populates a SQLite database with Goodreads book data. It includes functionality for storing and querying information about books, authors, genres, users, ratings, and reviews.

## Directory Structure

- `/data`: Contains the source data files
  - `goodreads_test.csv`: Test dataset
  - `goodreads_book_works.json`: Book works data in JSON format
  - `Goodreads_books_with_genres.csv`: Books with genre information

- `/db`: Directory for the SQLite database file
  - The database is created as `goodreads-db.sqlite`

- `/src`: Source code for database creation and operations
  - `create_db.py`: Creates the database schema with tables for books, authors, genres, users, ratings, and reviews
  - `db_fill.py`: Populates the database with data from CSV files
  - `db_functions.py`: Helper functions for database operations
  - `objects.py`: Object definitions for books, authors, etc.
  - `utils.py`: Utility functions

- `/notebooks`: Jupyter notebooks for data exploration
  - `Goodreads-db-exploration.ipynb`: Notebook for exploring the database

## Database Schema

The database consists of the following tables:

- `Books`: Book information (ID, title, ISBN, language, publication year, etc.)
- `Authors`: Author information
- `BookAuthors`: Many-to-many relationship between books and authors
- `Genres`: Genre categories
- `BookGenres`: Many-to-many relationship between books and genres
- `Users`: User information
- `Ratings`: User ratings for books
- `Reviews`: User reviews for books

## Getting Started

1. Clone this repository
2. Ensure you have Python installed with the required packages (pandas, numpy, sqlite3)
3. Run `python src/create_db.py` to create the database schema
4. Run `python src/db_fill.py` to populate the database with data
5. Use the Jupyter notebook for data exploration and analysis

## Usage

The database can be used for various analyses of book data, including:
- Book recommendation systems
- Genre analysis
- Author popularity metrics
- Rating distribution analysis
- User behavior analysis

## Requirements

- Python 3.x
- pandas
- numpy
- sqlite3
- Jupyter (for notebooks)
