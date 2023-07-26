import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import java.awt.*;
import java.sql.*;

public class ShowTable extends JFrame{
    private JTable table;
    private JPanel PanelTable;


    public ShowTable(){
        setContentPane(PanelTable);
        setTitle("Countries");
        setSize(550,420);
        setLocation(700,350);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection con= DriverManager.getConnection("jdbc:mysql://localhost:3306/Country","root","Alexandru");
            Statement st=con.createStatement();
            String query="select * from countries";
            ResultSet rs=st.executeQuery(query);
            ResultSetMetaData rsmd=rs.getMetaData();
            DefaultTableModel model= (DefaultTableModel) table.getModel();

            int columns=rsmd.getColumnCount();
            String[] ColumnNames=new String[columns];

            for(int i=0;i<columns;i++)
            {
                ColumnNames[i] = rsmd.getColumnName(i + 1);
            }

            model.setColumnIdentifiers(ColumnNames);
            String Reciving_CC,Sending_CC,Count;
            while(rs.next()){
                Reciving_CC=rs.getString(1);
                Sending_CC=rs.getString(2);
                Count=rs.getString(3);
                Object[] row={Reciving_CC,Sending_CC,Count};
                model.addRow(row);
            }
            st.close();
            con.close();

        } catch (ClassNotFoundException | SQLException e1) {
            e1.printStackTrace();
        }

        JTableHeader header= table.getTableHeader();
        header.setBackground(new Color(151, 201, 181));
        table.setBackground(new Color(111, 143, 97));
        header.setFont(new Font("JetBrains Mono",Font.PLAIN,14));
    }

   public static void main(String[] args) {
        ShowTable CountryTable =  new ShowTable();
    }
}