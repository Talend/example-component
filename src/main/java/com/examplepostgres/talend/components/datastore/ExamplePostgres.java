package com.examplepostgres.talend.components.datastore;

import java.sql.Statement;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.completion.SuggestionValues;

@DataStore("ExamplePostgres")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "url" }),
    @GridLayout.Row({ "port" }),
    @GridLayout.Row({ "username" }),
    @GridLayout.Row({ "password" }),
    @GridLayout.Row({ "database" })
})
@Checkable("validateConnection")
@Documentation("TODO fill the documentation for this configuration")
public class ExamplePostgres implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String url;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private int port;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String username;

    @Credential
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String password;

    @Option
    @Documentation("Database on Postgres server")
    private String database;

    public String getUrl() {
        return url;
    }

    public ExamplePostgres setUrl(String url) {
        this.url = url;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ExamplePostgres setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ExamplePostgres setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ExamplePostgres setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public ExamplePostgres setDatabase(String database)
    {
        this.database = database;
        return this;
    }


    public Connection getConnection() throws Exception
    {
        Connection conn = null;
        conn = DriverManager.getConnection("jdbc:postgresql://"+this.getUrl()+":"+this.getPort()+"/"+this.getDatabase(),this.getUsername(), this.getPassword());
        return conn;
    }

    public List<SuggestionValues.Item> getTables() {
    	List<SuggestionValues.Item> tables = new ArrayList<SuggestionValues.Item>();
    	try {
    		Connection conn = this.getConnection();
    		Statement st = conn.createStatement();
    		ResultSet rs = st.executeQuery("select concat(schemaname,'.',tablename) from pg_catalog.pg_tables where tableowner = '"+this.getDatabase()+"' order by schemaname, tablename");
    		while (rs.next())
    		{
    		    tables.add(new SuggestionValues.Item(rs.getString(1),rs.getString(1)));
    		}
    		rs.close();
    		st.close();
    		conn.close();
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	
        return tables;
    }
}