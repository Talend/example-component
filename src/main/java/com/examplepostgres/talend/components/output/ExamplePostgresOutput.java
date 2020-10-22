package com.examplepostgres.talend.components.output;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import com.examplepostgres.talend.components.service.ExampleComponentService;

@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(value = CUSTOM, custom = "ExamplePostgres") // icon is located at src/main/resources/icons/ExamplePostgres.svg
@Processor(name = "ExamplePostgresOutput")
@Documentation("TODO fill the documentation for this processor")
public class ExamplePostgresOutput implements Serializable {
    private final ExamplePostgresOutputConfiguration configuration;
    private final ExampleComponentService service;
    private List<Record> records = null;
    public ExamplePostgresOutput(@Option("configuration") final ExamplePostgresOutputConfiguration configuration,
                          final ExampleComponentService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
        // Note: if you don't need it you can delete it
        records = new ArrayList<Record>();
    }

    @BeforeGroup
    public void beforeGroup() {
        // if the environment supports chunking this method is called at the beginning if a chunk
        // it can be used to start a local transaction specific to the backend you use
        // Note: if you don't need it you can delete it
        records.clear();
    }

    @ElementListener
    public void onNext(
            @Input final Record defaultInput) {
        // this is the method allowing you to handle the input(s) and emit the output(s)
        // after some custom logic you put here, to send a value to next element you can use an
        // output parameter and call emit(value).
        records.add(defaultInput);
    }

    @AfterGroup
    public void afterGroup() {
        // symmetric method of the beforeGroup() executed after the chunk processing
        // Note: if you don't need it you can delete it
        this.configuration.getDataset().insertRecords(records);
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
        // Note: if you don't need it you can delete it
        if (!records.isEmpty()) this.configuration.getDataset().insertRecords(records);
    }
}