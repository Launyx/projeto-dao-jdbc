package model.dao.Impl;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SellerDaoJDBC implements SellerDao {

    private Connection conn;

    public SellerDaoJDBC(Connection conn){
        this.conn = conn;
    }

    @Override
    public void insert(Seller obj) {

    }

    @Override
    public void update(Seller obj) {

    }

    @Override
    public void deleteById(Integer id) {

    }

    @Override
    public Seller findById(Integer id) {
        PreparedStatement st = null;
        ResultSet rs = null;

        try{
            // Implementação da consulta (statement) no bando de dados
            st = conn.prepareStatement(
                    "SELECT seller.*, department.Name as DepName "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.DepartmentId = department.Id "
                    + "WHERE seller.Id = ? ");

            // Definindo valor a subtituir '?' no statement
            st.setInt(1, id);

            // Execução da consulta (statement) no banco e armazenamento da tabela resultante na variável rs (ResultSet)
            rs = st.executeQuery();

            /* Quando é executada uma consulta (statement) e o resultado é colocado em um ResultSet, este rs aponta
             para a posição 0, que não contém objeto. Por isso é utilizado um if com rs.next() para testar se houve
             algum resultado como retorno
             */
            if (rs.next()){
                // Chamanda do método que instancia um Department
                Department dep = instantiateDepartment(rs);

                // Chamanda do método que instancia um Seller
                Seller obj = instantiateSeller(rs, dep);
                return obj;
            }
            // Retorna nulo caso não houver retorno de objeto (não há seller com a consulta determinada)
            return null;
        }catch (SQLException e){
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(st);
            DB.closeResultSet(rs);
        }
    }

    /* Possível excessão de .gets propagadas com "throws SQLException" pois,
        O código que chama esse método já está tratando uma possível SQLException
    */
    private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException{
        Seller obj = new Seller();
        obj.setId(rs.getInt("iD"));
        obj.setName(rs.getString("Name"));
        obj.setEmail(rs.getString("Email"));
        obj.setBaseSalary(rs.getDouble("BaseSalary"));
        obj.setBirthDate(rs.getDate("BirthDate"));
        obj.setDepartment(dep);
        return obj;
    }

    /* Possível excessão de .gets propagadas com "throws SQLException" pois,
        O código que chama esse método já está tratando uma possível SQLException
    */
    private Department instantiateDepartment(ResultSet rs) throws SQLException{
        Department dep = new Department();
        dep.setId(rs.getInt("DepartmentId"));
        dep.setName(rs.getString("DepName"));
        return dep;
    }

    @Override
    public List<Seller> findAll() {
        return List.of();
    }
}
