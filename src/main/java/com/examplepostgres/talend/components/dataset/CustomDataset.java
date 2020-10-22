package com.examplepostgres.talend.components.dataset;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import com.examplepostgres.talend.components.datastore.ExamplePostgres;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

@DataSet("CustomDataset")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "table" })
})
@Documentation("TODO fill the documentation for this configuration")
public class CustomDataset implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ExamplePostgres datastore;

    @Option
    @Suggestable(value="showTables", parameters = {"datastore"})
    @Documentation("TODO fill the documentation for this parameter")
    private String table;

    public ExamplePostgres getDatastore() {
        return datastore;
    }

    public CustomDataset setDatastore(ExamplePostgres datastore) {
        this.datastore = datastore;
        return this;
    }

    public String getTable() {
        return table;
    }

    public CustomDataset setTable(String table) {
        this.table = table;
        return this;
    }

    public void insertRecords(List<Record> records) {
    	StringBuilder stmt = new StringBuilder();
		stmt.append("insert into " + this.getTable() + "(");
    	
    	stmt.append(this.getColumnNames(records.get(0).getSchema()));
    	stmt.append(") values ");
    	int o = 1;
    	for (Record record : records) {
    		Schema schema = record.getSchema();
    		
    		int i = 1;
    		stmt.append("(");
    		for (Schema.Entry entry : schema.getEntries())
    		{
    			if (i == schema.getEntries().size())
    				stmt.append("'"+record.getString(entry.getName()) +"'");
    			else
    				stmt.append("'"+record.getString(entry.getName())+"',");
    			i++;
    		}	
    		if (o == records.size())
    			stmt.append(")");
    		else
    			stmt.append("),");
    		o++;
    	}
    	Connection conn;
    	
		try {
			conn = this.getDatastore().getConnection();
			Statement st = conn.createStatement();
			st.execute(stmt.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }

    private String getColumnNames(Schema schema)
    {
    	StringBuilder buff = new StringBuilder();
    	int i = 1;
		for (Schema.Entry entry : schema.getEntries())
		{
			if (i == schema.getEntries().size())
				buff.append(entry.getName());
			else
				buff.append(entry.getName()+",");
			i++;
		}
		
		return buff.toString();
    }
}