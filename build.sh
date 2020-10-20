mvn clean install
rm /Applications/TalendStudio-7.2.1/studio/configuration/.m2/repository/com/example-postgres/example-component/1.0.0/example-component-1.0.0.jar
mvn talend-component:deploy-in-studio -Dtalend.component.enforceDeploy=true -Dtalend.component.studioHome="/Applications/TalendStudio-7.2.1/studio"