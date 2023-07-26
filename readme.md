# Take Home Assignment

### Data Context

Some of the pipes used by utility companies to transport gas is prone to leaks due to various reasons (e.g., pipe is old or pipe is prone to rust etc.). Utility companies collect data about leaks and various other information to repair these leaks. Leaks can be classified in various categories depending on how serious the leak is so that they can prioritize repairs for the more serious leaks.

### Goal

The goal of your take home assignment is to write code to ingest gas leak data from a SQLite database and save off the ingested data into another SQLite database.

### Data

We have provided a SQLite database with four tables as described below:

#### leaks

| Column        | Definition                                                          |
| ------------- | ------------------------------------------------------------------- |
| leak_no       | Unique identifier for the leak                                      |
| create_date   | The date and time when the record was created.                      |
| rpt_date      | The date and time when the leak was reported.                       |
| modified_date | The date and time when the record was modified.                     |
| class         | The leak class (1 represents the most serious leak and 3 the least) |
| pressure      | The pressure in the pipe                                            |
| town_code     | The town where the leak was found.                                  |
| company_code  | The utility company that owns the pipe.                             |

#### leak_class_min_date

_After a leak has been identified it is classified as class 1, 2, 2A or 3 (with 1 being the most serious and 3 the least). However, utility companies periodically inspect the pipes as the leak can become worse or diminish over time. Thus, the leak classification can change over time. The ‘leaks’ table gives the current class of the leak whereas the ‘leak_class_min_date’ table gives the leak class as it was classified when the leak was discovered originally._

| Column         | Definition                                            |
| -------------- | ----------------------------------------------------- |
| leak_no        | Unique identifier for the leak                        |
| class          | The original class of the leak.                       |
| class_date_min | The date and time when the leak was first classified. |

#### lut_company

_A look up table that associates a company code with the name of the company._

| Column       | Definition                        |
| ------------ | --------------------------------- |
| company_code | The code for the utility company. |
| description  | The name of the utility company.  |

#### town_company

_A look up table that has the company code, the town code and a market share column that indicates whether the company serves that town or not._

| Column       | Definition                                                                        |
| ------------ | --------------------------------------------------------------------------------- |
| company_code | The code for the utility company.                                                 |
| town_code    | The code for a town.                                                              |
| market_share | Values greater than or equal to 1 mean that the utility company serves that town. |

### Data Ingestion Requirements

- Data ingestion takes place every Friday at 8am BST.

- Data should be ingested on an incremental basis. In other words, do not do a complete data refresh but just pull the data that has not been ingested as of the date when the script runs.

- Exclude all records where company code is ‘ENI’.

- Only include records where a utility company serves the town (i.e., ‘market_share’ must be greater than or equal to 1).

- The output table should have the following columns:

  - 'leak_no'
  - 'create_date'
  - 'rpt_date'
  - 'modified_date'
  - 'current_class'
  - 'original_class'
  - 'pressure'
  - ‘cover'
  - 'town_code'
  - 'company_code'
  - 'company_name'

- You should use whatever logic is needed to get the original classification of the leak from the ‘leak_class_min_date’ table and rename that column as ‘original_class’.

- Rename the existing class in the ‘leaks’ table as ‘current_class’.

- Get the name of the utility company from the ‘lut_company’ table. Ensure that the name of the column is ‘company_name’.

### Code Expectations

Save the output table in another SQLite database and send that database file with your code.

Please use Python to complete the assignment and include appropriate tests.

Please explicitly state any assumptions you make either in the code or in a separate document.

Only spend up to a maximum of 3-4 hours on this exercise.
