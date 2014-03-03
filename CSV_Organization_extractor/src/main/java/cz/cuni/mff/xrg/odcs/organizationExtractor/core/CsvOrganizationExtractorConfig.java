package cz.cuni.mff.xrg.odcs.organizationExtractor.core;

import cz.cuni.mff.xrg.odcs.commons.module.config.DPUConfigObjectBase;
import cz.cuni.mff.xrg.odcs.rdf.enums.FileExtractType;
import cz.cuni.mff.xrg.odcs.rdf.enums.RDFFormatType;

/**
 * File extractor configuration.
 * 
 * @author Jan Marcek
 * 
 */
public class CsvOrganizationExtractorConfig extends DPUConfigObjectBase {
    public CsvOrganizationExtractorConfig(String path, RDFFormatType RDFFormatValue, String fileSuffix, FileExtractType fileExtractType,
            boolean onlyThisSuffix, boolean useStatisticalHandler, boolean failWhenErrors, Integer debugProcessOnlyNItems, Integer batchSize) {
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

    public CsvOrganizationExtractorConfig() {
    }

    public String Path = "e:/eea/comsode/rdf/source/organisations-dump.csv";
    public RDFFormatType RDFFormatValue = RDFFormatType.AUTO;
    public String FileSuffix = "";

    public FileExtractType fileExtractType = FileExtractType.PATH_TO_FILE;
    public boolean OnlyThisSuffix = false;
    public boolean UseStatisticalHandler = true;
    public boolean failWhenErrors = false;
    public Integer DebugProcessOnlyNItems = 10000;
    public Integer BatchSize = 10000;

    @Override
    public boolean isValid() {
        return Path != null && FileSuffix != null && RDFFormatValue != null && fileExtractType != null && DebugProcessOnlyNItems != null && BatchSize != null;
    }
}
