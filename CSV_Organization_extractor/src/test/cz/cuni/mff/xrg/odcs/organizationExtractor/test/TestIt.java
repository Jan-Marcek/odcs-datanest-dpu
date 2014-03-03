package cz.cuni.mff.xrg.odcs.organizationExtractor.test;

import cz.cuni.mff.xrg.odcs.dpu.test.TestEnvironment;
import cz.cuni.mff.xrg.odcs.organizationExtractor.core.CsvOrganizationExtractor;
import cz.cuni.mff.xrg.odcs.organizationExtractor.core.CsvOrganizationExtractorConfig;
import cz.cuni.mff.xrg.odcs.rdf.enums.FileExtractType;
import cz.cuni.mff.xrg.odcs.rdf.exceptions.RDFException;
import cz.cuni.mff.xrg.odcs.rdf.interfaces.RDFDataUnit;

public class TestIt {

    @org.junit.Test
    public void test() throws Exception, RDFException {
        CsvOrganizationExtractor extractor = new CsvOrganizationExtractor();
        CsvOrganizationExtractorConfig config = new CsvOrganizationExtractorConfig();
        config.DebugProcessOnlyNItems = 10;
        String fileUrl = "file:\\e:\\eea\\comsode\\dataset\\org\\organization_small.csv";
        String remoteUrl = "http://localhost:8000/organization_small.csv";
        config.Path = remoteUrl;

        config.fileExtractType = FileExtractType.HTTP_URL;
        extractor.configureDirectly(config);

        TestEnvironment env = TestEnvironment.create();
        try {
            RDFDataUnit output = env.createRdfOutput("output", false);
            // run the execution
            String input = null;
            env.run(extractor);
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            // release resources
            env.release();
        }
    }
}
