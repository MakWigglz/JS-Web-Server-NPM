const sqlite3 = require('sqlite3').verbose();
const DB_PATH = './user_data.sqlite'; // The file path for your database

// 1. Connect to the database (or create it if it doesn't exist)
const db = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
        console.error('Error opening database:', err.message);
    } else {
        console.log('Connected to the SQLite database: ' + DB_PATH);
        createTableAndInsertData();
    }
});

function createTableAndInsertData() {
    db.serialize(() => {
        // 2. Create the 'users' table
        // 'id' is our new index column, set as INTEGER PRIMARY KEY for auto-incrementing
        db.run(`CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            login_email TEXT NOT NULL UNIQUE, -- Login email should be unique
            identifier TEXT,
            first_name TEXT,
            last_name TEXT
        )`, (err) => {
            if (err) {
                console.error('Error creating users table:', err.message);
            } else {
                console.log('Table "users" checked/created.');
                insertInitialData();
            }
        });
    });
}

function insertInitialData() {
    // Check if the table is empty before inserting to avoid duplicates on re-runs
    db.get("SELECT COUNT(*) AS count FROM users", (err, row) => {
        if (err) {
            console.error('Error checking user count:', err.message);
            return;
        }

        if (row.count === 0) {
            console.log('Inserting initial data into "users" table...');
            const stmt = db.prepare(`INSERT INTO users (username, login_email, identifier, first_name, last_name)
                                     VALUES (?, ?, ?, ?, ?)`);

            // Sample Data Rows
            stmt.run("johndoe", "john.doe@example.com", "JD001", "John", "Doe");
            stmt.run("janesmith", "jane.smith@example.com", "JS002", "Jane", "Smith");
            stmt.run("petersjones", "peter.jones@example.com", "PJ003", "Peter", "Jones");
            stmt.run("aliciak", "alicia.k@example.com", "AK004", "Alicia", "Kim");
            stmt.run("davidm", "david.m@example.com", "DM005", "David", "Miller");

            stmt.finalize((err) => {
                if (err) {
                    console.error('Error inserting data:', err.message);
                } else {
                    console.log('Initial data inserted successfully.');
                    readAllData(); // Read data after insertion
                }
            });
        } else {
            console.log('Users table already contains data. Skipping initial insert.');
            readAllData(); // Read existing data
        }
    });
}

function readAllData() {
    console.log('\n--- Current Users Table Data ---');
    db.each("SELECT id, username, login_email, identifier, first_name, last_name FROM users", (err, row) => {
        if (err) {
            console.error('Error reading data:', err.message);
        } else {
            console.log(`ID: ${row.id}, Username: ${row.username}, Email: ${row.login_email}, ` +
                        `Identifier: ${row.identifier}, Name: ${row.first_name} ${row.last_name}`);
        }
    }, () => {
        // This callback fires after all rows have been processed
        console.log('--- End of Data ---');
        db.close((err) => {
            if (err) {
                console.error('Error closing database:', err.message);
            } else {
                console.log('Database connection closed.');
            }
        });
    });
}
