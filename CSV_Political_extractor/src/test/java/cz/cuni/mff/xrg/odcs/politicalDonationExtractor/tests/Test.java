package cz.cuni.mff.xrg.odcs.politicalDonationExtractor.tests;

import cz.cuni.mff.xrg.odcs.commons.configuration.ConfigException;
import cz.cuni.mff.xrg.odcs.dpu.test.TestEnvironment;
import cz.cuni.mff.xrg.odcs.politicalDonationExtractor.core.CsvPoliticalExtractor;
import cz.cuni.mff.xrg.odcs.politicalDonationExtractor.core.CsvPoliticalExtractorConfig;
import cz.cuni.mff.xrg.odcs.rdf.enums.FileExtractType;
import cz.cuni.mff.xrg.odcs.rdf.interfaces.RDFDataUnit;

import java.net.URL;

public class Test {

    @org.junit.Test
    public void test() throws ConfigException {
        CsvPoliticalExtractor extractor = new CsvPoliticalExtractor();
        CsvPoliticalExtractorConfig config = new CsvPoliticalExtractorConfig();
        config.DebugProcessOnlyNItems = 10;
        URL url = this.getClass().getResource("/political-dump.csv");
        String remoteUrl = "http://localhost:8000/political-dump.csv";

        String fileUrl = url.getPath();
        config.Path = fileUrl;
        config.fileExtractType = FileExtractType.UPLOAD_FILE;
        TestEnvironment env = TestEnvironment.create();
        extractor.configureDirectly(config);

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
