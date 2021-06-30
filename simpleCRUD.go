package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
)

var db *sql.DB

var server = "localhost"
var port = 1433
var user = ""
var password = ""
var database = ""

func main() {
	connString := fmt.Sprintf("Connection String")

	var err error

	//Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool...", err.Error())
	}

	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Printf("Connected\n")

	//Create customer
	createID, err := CreateCustomer("Fravega")
	if err != nil {
		log.Fatal("Error creating customer", err.Error())
	}
	fmt.Printf("Inserted ID: %d\n", createID)

	//read Customer
	count, err := ReadCustomers()
	if err != nil {
		log.Fatal("Error reading customers\n", err.Error())
	}
	fmt.Printf("Read %d rows successfully\n", count)

	//Update from database
	updateRows, err := UpdateCustomer(3, "Garbarino")
	if err != nil {
		log.Fatal("Error updating customer", err.Error())
	}
	fmt.Printf("Updated %d row(s) successfully\n", updateRows)

	//delete from database
	deletedRows, err := DeleteCustomer(3, "Garbarino")
	if err != nil {
		log.Fatal("Error deleting rows", err.Error())
	}
	fmt.Printf("Deleted %d row(s) successfully\n", deletedRows)

}

func CreateCustomer(name string) (int64, error) {
	ctx := context.Background()
	var err error

	if db == nil {
		err = errors.New("CreateCustomer db is null")
		return -1, err
	}

	//Check is database is alive
	err = db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := `
		INSERT into Customer (name) values(@name);
		select isNull(SCOPE_IDENTITY), -1;
		`
	stmt, err := db.Prepare(tsql)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()

	row := stmt.QueryRowContext(
		ctx,
		sql.Named("Name", name),
	)
	var newID int64
	err = row.Scan(&newID)
	if err != nil {
		return -1, err
	}
	return newID, nil
}

func ReadCustomers() (int, error) {
	ctx := context.Background()

	//Check is database is alive
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := fmt.Sprintf("Select ID, name from Customers;")

	//Execute query
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	var count int

	//Iterate through the result set
	for rows.Next() {
		var name string
		var id int

		//Get values from row
		err := rows.Scan(&id, &name)
		if err != nil {
			return -1, err
		}
		fmt.Printf("ID: %d, Name: %s\n", id, name)
		count++
	}
	return count, nil
}

func UpdateCustomer(id int64, name string) (int64, error) {
	ctx := context.Background()
	//Check is database is alive
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf("Update Customers set name=@name WHERE Id=@id;")

	//Execute query
	result, err := db.ExecContext(
		ctx,
		tsql,
		sql.Named("Name", name),
	)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

//Delete customers
func DeleteCustomer(id int64, name string) (int64, error) {
	ctx := context.Background()

	//check is db alive
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf("DELETE from Customers where id=@id;")

	//Execute non-query with named parameters
	result, err := db.ExecContext(
		ctx,
		tsql,
		sql.Named("Name", name),
	)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()

}
