package cz.cuni.mff.xrg.odcs.politicalDonationExtractor.core;

import cz.cuni.mff.xrg.odcs.commons.module.config.DPUConfigObjectBase;
import cz.cuni.mff.xrg.odcs.rdf.enums.FileExtractType;
import cz.cuni.mff.xrg.odcs.rdf.enums.RDFFormatType;

/**
 * File extractor configuration.
 * 
 * @author Jan Marcek
 * 
 */
public class CsvPoliticalExtractorConfig extends DPUConfigObjectBase {
    String url = "";
    String file = "";
    public String Path = url;
    public RDFFormatType RDFFormatValue = RDFFormatType.AUTO;
    public String FileSuffix = "";

    public FileExtractType fileExtractType = FileExtractType.PATH_TO_FILE;
    public boolean OnlyThisSuffix = false;
    public boolean UseStatisticalHandler = true;
    public boolean failWhenErrors = false;
    public Integer DebugProcessOnlyNItems = new Integer(10000);
    public Integer BatchSize = new Integer(10000);

    public CsvPoliticalExtractorConfig() {
    }

    public CsvPoliticalExtractorConfig(String path, RDFFormatType RDFFormatValue, String fileSuffix, FileExtractType fileExtractType, boolean onlyThisSuffix,
            boolean useStatisticalHandler, boolean failWhenErrors, Integer debugProcessOnlyNItems, Integer batchSize) {
        Path = path;
        this.RDFFormatValue = RDFFormatValue;
        FileSuffix = fileSuffix;
        this.fileExtractType = fileExtractType;
        OnlyThisSuffix = onlyThisSuffix;
        UseStatisticalHandler = useStatisticalHandler;
        this.failWhenErrors = failWhenErrors;
        DebugProcessOnlyNItems = debugProcessOnlyNItems;
        BatchSize = batchSize;
    }

    @Override
    public boolean isValid() {
        return Path != null && FileSuffix != null && RDFFormatValue != null && fileExtractType != null && DebugProcessOnlyNItems != null && BatchSize != null;
    }
}
