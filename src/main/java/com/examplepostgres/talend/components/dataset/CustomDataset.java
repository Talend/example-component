package com.examplepostgres.talend.components.dataset;

import java.io.Serializable;

import com.examplepostgres.talend.components.datastore.ExamplePostgres;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

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
}