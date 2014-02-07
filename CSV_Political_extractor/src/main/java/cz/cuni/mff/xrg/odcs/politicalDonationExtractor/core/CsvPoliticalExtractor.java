package cz.cuni.mff.xrg.odcs.politicalDonationExtractor.core;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.xrg.odcs.commons.data.DataUnitException;
import cz.cuni.mff.xrg.odcs.commons.dpu.DPUContext;
import cz.cuni.mff.xrg.odcs.commons.dpu.DPUException;
import cz.cuni.mff.xrg.odcs.commons.dpu.annotation.AsExtractor;
import cz.cuni.mff.xrg.odcs.commons.dpu.annotation.OutputDataUnit;
import cz.cuni.mff.xrg.odcs.commons.message.MessageType;
import cz.cuni.mff.xrg.odcs.commons.module.dpu.ConfigurableBase;
import cz.cuni.mff.xrg.odcs.commons.web.AbstractConfigDialog;
import cz.cuni.mff.xrg.odcs.commons.web.ConfigDialogProvider;
import cz.cuni.mff.xrg.odcs.politicalDonationExtractor.datanest.AbstractDatanestHarvester;
import cz.cuni.mff.xrg.odcs.politicalDonationExtractor.datanest.PoliticalPartyDonationsDatanestHarvester;
import cz.cuni.mff.xrg.odcs.rdf.enums.FileExtractType;
import cz.cuni.mff.xrg.odcs.rdf.enums.HandlerExtractType;
import cz.cuni.mff.xrg.odcs.rdf.enums.RDFFormatType;
import cz.cuni.mff.xrg.odcs.rdf.exceptions.RDFException;
import cz.cuni.mff.xrg.odcs.rdf.interfaces.RDFDataUnit;

/**
 * @author Jan Marcek
 */
@AsExtractor
public class CsvPoliticalExtractor extends ConfigurableBase<CsvPoliticalExtractorConfig> implements ConfigDialogProvider<CsvPoliticalExtractorConfig> {

    private final Logger LOG = LoggerFactory.getLogger(CsvPoliticalExtractor.class);
    public final static String MODULE_NAME = "Csv_Political_Extractor";

    public CsvPoliticalExtractor() {
        super(CsvPoliticalExtractorConfig.class);
    }

    @OutputDataUnit
    public RDFDataUnit rdfDataUnit;

    @Override
    public void execute(DPUContext context) throws DataUnitException, DPUException {
        final String baseURI = "";
        final FileExtractType extractType = config.fileExtractType;
        final String sourceCSV = config.Path;
        final Integer batchSize = config.BatchSize;
        final Integer debugProcessOnlyNItems = config.DebugProcessOnlyNItems;
        final String fileSuffix = config.FileSuffix;
        final boolean onlyThisSuffix = config.OnlyThisSuffix;
        boolean useStatisticHandler = config.UseStatisticalHandler;
        boolean failWhenErrors = config.failWhenErrors;
        RDFFormatType formatType = config.RDFFormatValue;
        final RDFFormat format = RDFFormatType.getRDFFormatByType(formatType);
        final HandlerExtractType handlerExtractType = HandlerExtractType.getHandlerType(useStatisticHandler, failWhenErrors);

        LOG.debug("extractType: {}", extractType);
        LOG.debug("format: {}", format);
        LOG.debug("fileSuffix: {}", fileSuffix);
        LOG.debug("baseURI: {}", baseURI);
        LOG.debug("onlyThisSuffix: {}", onlyThisSuffix);
        LOG.debug("useStatisticHandler: {}", useStatisticHandler);

        try {
            AbstractDatanestHarvester<?> harvester = null;
            URL sourceUrl = getSourceUrl(sourceCSV);
            File workingDirDpu = getWorkingDirDpu(context);
            LOG.debug("path: {}", workingDirDpu.getAbsolutePath());
            performET(context, batchSize, debugProcessOnlyNItems, sourceUrl, workingDirDpu);
            File[] files = getFiles(workingDirDpu);
            exportFiles(context, baseURI, extractType, fileSuffix, onlyThisSuffix, format, handlerExtractType, files);
            FileUtils.deleteDirectory(workingDirDpu);
            long triplesCount = rdfDataUnit.getTripleCount();
            LOG.info("A harvesting is successfully finished : " + triplesCount);
        } catch (Exception e) {
            LOG.error("Error", e);
            context.sendMessage(MessageType.ERROR, e.getMessage());
            throw new DPUException(e.getMessage(), e);
        }
    }

    private File getWorkingDirDpu(DPUContext context) {
        File workingDir = context.getWorkingDir();
        return new File(workingDir.getAbsolutePath() + "/" + MODULE_NAME + "/");
    }

    private URL getSourceUrl(String sourceCSV) {
        URL url = null;
        try {
            url = new URL(sourceCSV);
        } catch (IOException e) {
            LOG.error("An error occoured when path: " + sourceCSV + " was parsing.", e);
        }
        return url;
    }

    private void performET(DPUContext context, Integer batchSize, Integer debugProcessOnlyNItems, URL url, File workingDirDpu) {
        AbstractDatanestHarvester<?> harvester;
        try {
            harvester = new PoliticalPartyDonationsDatanestHarvester(workingDirDpu.getAbsolutePath());
            harvester.setDebugProcessOnlyNItems(debugProcessOnlyNItems);
            harvester.setBatchSize(batchSize);
            harvester.setSourceUrl(url);
            LOG.info("A harvesting starts");
            harvester.update();
        } catch (Exception e) {
            LOG.error("A problem occoured when a transformation csv -> rdf was performing", e);
            context.sendMessage(MessageType.ERROR, e.getMessage());
        }
    }

    private void exportFiles(DPUContext context, String baseURI, FileExtractType extractType, String fileSuffix, boolean onlyThisSuffix, RDFFormat format,
            HandlerExtractType handlerExtractType, File[] files) {
        for (File tmpRdf : files) {
            try {
                String path = tmpRdf.toURI().toURL().toExternalForm();
                LOG.debug("rdf file: " + path);
                rdfDataUnit.extractFromFile(extractType, format, path, fileSuffix, baseURI, onlyThisSuffix, handlerExtractType);
            } catch (RDFException e) {
                LOG.error("An error occoured when export was performing. A file: " + tmpRdf.getAbsolutePath(), e);
                context.sendMessage(MessageType.ERROR, e.getMessage());
            } catch (MalformedURLException e) {
                LOG.error("An error occoured when export was performing. A file: " + tmpRdf.getAbsolutePath(), e);
                context.sendMessage(MessageType.ERROR, e.getMessage());
            }
        }
    }

    private File[] getFiles(File workingDirDpu) {
        FilenameFilter directoryFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isFile();
            }
        };

        return workingDirDpu.listFiles(directoryFilter);
    }

    @Override
    public AbstractConfigDialog<CsvPoliticalExtractorConfig> getConfigurationDialog() {
        return new CsvPoliticalExtractorDialog();
    }

}
