package application;

import db.DB;
import db.DbIntegrityException;
import model.entities.Department;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Program {
    public static void main(String[] args) {

        Department obj = new Department(1, "Books");
        System.out.println(obj);
    }
}
