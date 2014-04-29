import com.mysql.jdbc.Connection;
import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.PreparedStatement;
import java.io.*;
import java.net.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;


public class servidorbd{
    static Connection connection;

    static void conectar(String base){
        try{
            connection = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3307/"+base,"root","qwerty");
        }catch (SQLException e){
            e.printStackTrace();
        return;
        }        
    }
        public static void main(String args[]) throws Exception {
        String clientSentence;
        String capitalizedSentence = "";
        ServerSocket welcomeSocket = new ServerSocket(6789);
        PreparedStatement ps = null;

        while(true) {
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            clientSentence = inFromClient.readLine();
            String[] cachos = clientSentence.split("::");
            System.out.println(cachos[1]);
            conectar(cachos[1]);
            
            if(cachos[0].equals("list")){
                ps = (PreparedStatement) connection.prepareStatement("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?");
                ps.setString(1, cachos[1]);
                ResultSet rs = ps.executeQuery();
                while (rs.next()){
                    capitalizedSentence += rs.getString(1)+";";
                }
                capitalizedSentence+= '\n';
                outToClient.writeBytes(capitalizedSentence + "\n");
            }
            
            if(cachos[0].equals("num")){
                ps = (PreparedStatement) connection.prepareStatement("SELECT COUNT(*) AS total FROM "+cachos[1]+'.'+cachos[2]);
                ResultSet rs = ps.executeQuery();
                while (rs.next()){
                    capitalizedSentence = rs.getString("total");
                }
                outToClient.writeBytes(capitalizedSentence + "\n");
            }
            
            if(cachos[0].equals("ren")){
                ps = (PreparedStatement) connection.prepareStatement("RENAME TABLE "+cachos[1]+'.'+cachos[2]+" TO "+cachos[1]+'.'+cachos[3]);
                int renombrar = ps.executeUpdate();
                
                if(renombrar == 0){
                    capitalizedSentence = "Datos Borrados"+"\n";
                }else{
                    capitalizedSentence = "Algo Fallou"+"\n";
                }
                
                outToClient.writeBytes(capitalizedSentence + "\n");
            }
            
            if(cachos[0].equals("del")){
                ps = (PreparedStatement) connection.prepareStatement("DROP TABLE "+cachos[1]+'.'+cachos[2]);
                int borrar = ps.executeUpdate();
                
                if(borrar == 0){
                    capitalizedSentence = "Datos Borrados"+"\n";
                }else{
                    capitalizedSentence = "Algo Fallou"+"\n";
                }
                outToClient.writeBytes(capitalizedSentence + "\n");
            }
        }
    }
}