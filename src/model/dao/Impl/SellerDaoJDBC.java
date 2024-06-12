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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SellerDaoJDBC implements SellerDao {

    private Connection conn;

    public SellerDaoJDBC(Connection conn){
        this.conn = conn;
    }

    @Override
    public void insert(Seller obj) {

        PreparedStatement st = null;
        try{
            st = conn.prepareStatement(
                    "INSERT INTO seller "
                    + "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
                    + "VALUES "
                    + "(?, ?, ?, ?, ?)",
                    PreparedStatement.RETURN_GENERATED_KEYS);

            st.setString(1, obj.getName());
            st.setString(2, obj.getEmail());
            st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
            st.setDouble(4, obj.getBaseSalary());
            st.setDouble(5, obj.getDepartment().getId());

            int rowsAffected = st.executeUpdate();

            if (rowsAffected > 0){
                ResultSet rs = st.getGeneratedKeys();
                if(rs.next()){
                    int id = rs.getInt(1);
                    obj.setId(id);
                }
                DB.closeResultSet(rs);
            }else{
                throw new DbException("Unexpected error! No rows affected!");
            }

        }catch(SQLException e){
            throw new DbException(e.getMessage());
        }finally {
            DB.closeStatement(st);
        }
    }

    @Override
    public void update(Seller obj) {
        PreparedStatement st = null;
        try{
            st = conn.prepareStatement(
                    "UPDATE seller "
                            + "SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
                            + "WHERE Id = ? ",
                    PreparedStatement.RETURN_GENERATED_KEYS);

            st.setString(1, obj.getName());
            st.setString(2, obj.getEmail());
            st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
            st.setDouble(4, obj.getBaseSalary());
            st.setDouble(5, obj.getDepartment().getId());
            st.setInt(6, obj.getId());
            int rowsAffected = st.executeUpdate();

            if (rowsAffected > 0){
                ResultSet rs = st.getGeneratedKeys();
                if(rs.next()){
                    int id = rs.getInt(1);
                    obj.setId(id);
                }
                DB.closeResultSet(rs);
            }else{
                throw new DbException("Unexpected error! No rows affected!");
            }

        }catch(SQLException e){
            throw new DbException(e.getMessage());
        }finally {
            DB.closeStatement(st);
        }
    }

    @Override
    public void deleteById(Integer id) {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement(
                    "DELETE FROM seller "
                    + "WHERE Id = ? ");

            st.setInt(1, id);

            int rows = st.executeUpdate();

            if (rows == 0){
                throw new DbException("No rows were affected");
            }

        }catch(SQLException e){
            throw new DbException(e.getMessage());

        }finally {
            DB.closeStatement(st);
        }
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
        PreparedStatement st = null;
        ResultSet rs = null;

        try{
            // Implementação da consulta (statement) no bando de dados
            st = conn.prepareStatement(
                    "SELECT seller.*, department.Name as DepName "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.DepartmentId = department.Id "
                    + "ORDER BY Name ");

            // Execução da consulta (statement) no banco e armazenamento da tabela resultante na variável rs (ResultSet)
            rs = st.executeQuery();

            List<Seller> list = new ArrayList<>();

            Map<Integer, Department> map = new HashMap<>();

            while (rs.next()){

                // Procurando no map instanciado, se há algum objeto Department com a chave do DepartmentId
                Department dep = map.get(rs.getInt("DepartmentId"));

                if (dep == null){
                    // Chamanda do método que instancia um Department
                    dep = instantiateDepartment(rs);

                    /*  Alocando o departamento instanciado no map, para evitar a criação de múltiplos departamentos
                        com mesmo ID */

                    map.put(rs.getInt("DepartmentId"), dep);
                }

                // Chamanda do método que instancia um Seller
                Seller obj = instantiateSeller(rs, dep);

                list.add(obj);

            }
            return list;
        }catch (SQLException e){
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(st);
            DB.closeResultSet(rs);
        }
    }

    @Override
    public List<Seller> findByDepartment(Department department) {
        PreparedStatement st = null;
        ResultSet rs = null;

        try{
            // Implementação da consulta (statement) no bando de dados
            st = conn.prepareStatement(
                    "SELECT seller.*, department.Name as DepName "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.DepartmentId = department.Id "
                    + "WHERE DepartmentId = ? "
                    + "ORDER BY Name ");

            // Definindo valor a subtituir '?' no statement
            st.setInt(1, department.getId());

            // Execução da consulta (statement) no banco e armazenamento da tabela resultante na variável rs (ResultSet)
            rs = st.executeQuery();

            List<Seller> list = new ArrayList<>();

            Map<Integer, Department> map = new HashMap<>();

            /* Quando é executada uma consulta (statement) e o resultado é colocado em um ResultSet, este rs aponta
             para a posição 0, que não contém objeto. Por isso é utilizado um if com rs.next() para testar se houve
             algum resultado como retorno
             */
            while (rs.next()){

                // Procurando no map instanciado, se há algum objeto Department com a chave do DepartmentId
                Department dep = map.get(rs.getInt("DepartmentId"));

                if (dep == null){
                    // Chamanda do método que instancia um Department
                    dep = instantiateDepartment(rs);

                    /*  Alocando o departamento instanciado no map, para evitar a criação de múltiplos departamentos
                        com mesmo ID
                    */
                    map.put(rs.getInt("DepartmentId"), dep);
                }

                // Chamanda do método que instancia um Seller
                Seller obj = instantiateSeller(rs, dep);

                list.add(obj);

            }
            return list;
        }catch (SQLException e){
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(st);
            DB.closeResultSet(rs);
        }
    }
}