import java.io.*;
import java.sql.*;
import java.util.*;

class Lookup {
    static void validate(ResultSet r, long i) throws Exception {
        if (!r.next() || r.getLong(1) != i || r.getLong(2) != i % 32 || r.getLong(3) != i * 17 % 100000)
            throw new IllegalStateException("Unexpected order row " + i);
    }
    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String host = in.readLine();
        Properties p = new Properties();
        p.setProperty("user", in.readLine());
        p.setProperty("password", in.readLine());
        p.setProperty("sslmode", "verify-full");
        p.setProperty("sslfactory", "org.postgresql.ssl.DefaultJavaSSLFactory");
        p.setProperty("connectTimeout", "10");
        p.setProperty("socketTimeout", "30");
        String sql = "SELECT order_id, tenant_id, amount_cents FROM public.orders WHERE ";
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://" + host + ":5432/databricks_postgres",p);
             PreparedStatement warm = c.prepareStatement(sql + "order_id BETWEEN 37 AND 41 ORDER BY order_id DESC LIMIT 2");
             PreparedStatement point = c.prepareStatement(sql + "order_id = ?")) {
            try (ResultSet keys = c.getMetaData().getPrimaryKeys(null, "public", "orders")) {
                if (!keys.next() || !"order_id".equals(keys.getString("COLUMN_NAME")) || keys.next())
                    throw new IllegalStateException("Expected public.orders PRIMARY KEY (order_id)");
            }
            System.out.println("PK=order_id");
            for (int j = 0; j < 8; j++) {
                long i = 32000 + 7919 * j;
                try (ResultSet r = warm.executeQuery()) {
                    validate(r, 41);
                    validate(r, 40);
                    if (r.next()) throw new IllegalStateException("Warm-up row count");
                }
                point.setLong(1,i);
                long started = System.nanoTime();
                try (ResultSet r = point.executeQuery()) {
                    validate(r, i);
                    if (r.next()) throw new IllegalStateException("Unexpected row count");
                }
                System.out.printf(Locale.ROOT,"%d,%.4f%n",i,(System.nanoTime()-started)/1e6);
            }
        }
    }
}
