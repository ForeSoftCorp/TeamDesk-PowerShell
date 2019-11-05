# tdimport.ps1

Imports data from CSV file into TeamDesk

## Parameters
`-URL`
: URL to the database, such as https://www.teamdesk.net/secure/db/21995

`-u`, `-User`
: User's email or API token

`-p`, `-Password`
: User's password or empty string in case of token

`-f`, `-File`
: Path to CSV file

`-a`, `-Table`
: The name of the table in singular form to import data to

`-c`, `-Columns`
: An array of column names in database to import data to. use "x" to ignore column in the file. 
For example `-c "First Name",x,"Last Name"`

`-w`, `-Workflow`
: Switch. Whether to run workflow rules for each imported record. Default is false - do not run - requires Manage Data setup privilege from the user.

`-m`, `-Match`
: The name of the unique column to use to find and update the record. By default key column is used.

`-Skip N`
: Number of rows to skip in CSV file prior to starting the import. Default is 1, skip header row

`-l`, `-Culture`
: String in form of languageCode-CountryCode. Affects how dates and numbers are parsed in CSV file. Default is computer user's culture.

`-d`, `-Delimiter`
: Character that is used to delimit values in CSV file. Default is culture-specific list separator.

`-e`, `-Encoding`
: CSV file's text encoding if different from computer's default

`-v`, `-Verbose`
: Prints detailed progress information

`-wi`, `-WhatIf`
: Scans CSV file for errors but does not import the data
