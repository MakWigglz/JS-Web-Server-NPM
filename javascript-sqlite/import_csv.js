const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const sqlite3 = require('sqlite3').verbose();
const CSV_FILE_PATH = path.join(__dirname, 'users.csv');
const DB_PATH = path.join(__dirname, 'user_data.sqlite'); // Use the same database file
const START_ROW_INDEX = 6; // Rows are 0-indexed in JavaScript arrays.
                            // If you want to start from the 7th row, the index is 6.
                            // (Row 1 = index 0, Row 7 = index 6)

// --- Database Connection ---
const db = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
        console.error('Error opening database:', err.message);
    } else {
        console.log('Connected to the SQLite database: ' + DB_PATH);
        processCsvAndImport();
    }
});

function processCsvAndImport() {
    let rowCount = 0;
    let importedRowCount = 0;
    const usersToInsert = [];

    console.log(`Starting CSV parsing from row index ${START_ROW_INDEX + 1} (1-based).`);

    fs.createReadStream(CSV_FILE_PATH)
        .pipe(csv()) // Use csv-parser
        .on('data', (row) => {
            rowCount++;
            // Check if the current row index is greater than or equal to START_ROW_INDEX
            // csv-parser emits objects for each row, so 'rowCount' is useful here.
            // Note: csv-parser often handles headers implicitly and starts data rows from index 1 (or 0 for data).
            // For robust control over row numbers, it's better to use a counter.
            if (rowCount > START_ROW_INDEX) { // If START_ROW_INDEX is 6, this means row 7 onwards (after 6th row)
                // Map CSV columns to database table columns
                // Ensure your CSV headers match these keys, or adjust accordingly
                const user = {
                    username: row.username,
                    login_email: row.login_email,
                    identifier: row.identifier,
                    first_name: row.first_name,
                    last_name: row.last_name
                };

                // Basic validation (e.g., check for essential fields)
                if (user.username && user.login_email) {
                    usersToInsert.push(user);
                } else {
                    console.warn(`Skipping row ${rowCount}: Missing essential data (username or login_email).`, row);
                }
            }
        })
        .on('end', () => {
            console.log(`CSV file successfully processed. Total rows read (including headers): ${rowCount}`);
            console.log(`Rows prepared for import (from row ${START_ROW_INDEX + 1} onwards): ${usersToInsert.length}`);
            insertUsersIntoDb(usersToInsert);
        })
        .on('error', (error) => {
            console.error('Error reading CSV file:', error.message);
            db.close();
        });
}

function insertUsersIntoDb(users) {
    if (users.length === 0) {
        console.log('No users to insert. Closing database.');
        db.close();
        return;
    }

    db.serialize(() => {
        // Prepare a single INSERT statement
        // The 'id' column is AUTOINCREMENT, so we don't include it in the INSERT statement
        const stmt = db.prepare(`INSERT INTO users (username, login_email, identifier, first_name, last_name)
                                 VALUES (?, ?, ?, ?, ?)`);

        let importedCount = 0;
        let skippedCount = 0;

        // Iterate over the users and run the insert statement for each
        users.forEach((user, index) => {
            stmt.run(
                user.username,
                user.login_email,
                user.identifier,
                user.first_name,
                user.last_name,
                function(err) { // Use a regular function for 'this' context in sqlite3
                    if (err) {
                        // Handle errors, e.g., unique constraint violation for login_email
                        if (err.message.includes('UNIQUE constraint failed: users.login_email')) {
                            console.warn(`Skipping duplicate user (email: ${user.login_email}): ${err.message}`);
                            skippedCount++;
                        } else {
                            console.error(`Error inserting user ${user.username}: ${err.message}`);
                        }
                    } else {
                        // console.log(`Inserted user: ${user.username} with ID: ${this.lastID}`);
                        importedCount++;
                    }

                    // Check if this is the last item
                    if (index === users.length - 1) {
                        stmt.finalize((err) => {
                            if (err) {
                                console.error('Error finalizing statement:', err.message);
                            } else {
                                console.log(`\n--- Import Summary ---`);
                                console.log(`Total rows from CSV processed: ${users.length}`);
                                console.log(`Successfully imported: ${importedCount} rows.`);
                                console.log(`Skipped (e.g., duplicates): ${skippedCount} rows.`);
                                readAndDisplayAllUsers(); // Display all users after import
                            }
                        });
                    }
                }
            );
        });
    });
}

function readAndDisplayAllUsers() {
    console.log('\n--- All Users in Database After Import ---');
    db.all("SELECT id, username, login_email, identifier, first_name, last_name FROM users ORDER BY id ASC", (err, rows) => {
        if (err) {
            console.error('Error fetching all users:', err.message);
        } else {
            if (rows.length === 0) {
                console.log("No users found in the database.");
            } else {
                rows.forEach(row => {
                    console.log(`ID: ${row.id}, Username: ${row.username}, Email: ${row.login_email}, ` +
                                `Identifier: ${row.identifier || 'N/A'}, Name: ${row.first_name || 'N/A'} ${row.last_name || 'N/A'}`);
                });
            }
        }
        db.close((closeErr) => {
            if (closeErr) {
                console.error('Error closing database:', closeErr.message);
            } else {
                console.log('Database connection closed.');
            }
        });
    });
}
