package cz.cuni.mff.xrg.odcs.organizationExtractor.core;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

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
import cz.cuni.mff.xrg.odcs.organizationExtractor.datanest.AbstractDatanestHarvester;
import cz.cuni.mff.xrg.odcs.organizationExtractor.datanest.OrganizationsDatanestHarvester;
import cz.cuni.mff.xrg.odcs.rdf.enums.FileExtractType;
import cz.cuni.mff.xrg.odcs.rdf.enums.HandlerExtractType;
import cz.cuni.mff.xrg.odcs.rdf.enums.RDFFormatType;
import cz.cuni.mff.xrg.odcs.rdf.exceptions.RDFException;
import cz.cuni.mff.xrg.odcs.rdf.interfaces.RDFDataUnit;

/**
 * @author Jan Marcek
 */
// TODO change name of a class. Replace the "extractor" with something more appropriate

@AsExtractor
public class CsvOrganizationExtractor extends ConfigurableBase<CsvOrganizationExtractorConfig> implements ConfigDialogProvider<CsvOrganizationExtractorConfig> {

    private final Logger LOG = LoggerFactory.getLogger(CsvOrganizationExtractor.class);

    @OutputDataUnit
    public RDFDataUnit rdfDataUnit;

    public CsvOrganizationExtractor() {
        super(CsvOrganizationExtractorConfig.class);
    }

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
        File rdfDirectory = null;
        try {
            File globalDirectory  = context.getGlobalDirectory();
            Path path = globalDirectory.toPath();
            Path  rdfsPath = Files.createTempDirectory(path, "");
            LOG.debug("created a temp file. Path: " + rdfsPath.toAbsolutePath());
            rdfDirectory = rdfsPath.toFile();
            AbstractDatanestHarvester<?> harvester = null;
            URL sourceUrl = getSourceUrl(sourceCSV);
            performET(context, batchSize, debugProcessOnlyNItems, sourceUrl, rdfDirectory);
            File[] files = getFiles(rdfDirectory);
            exportFiles(context, baseURI, extractType, fileSuffix, onlyThisSuffix, format, handlerExtractType, files);
        } catch (Exception e) {
            LOG.error("Error", e);
            context.sendMessage(MessageType.ERROR, e.getMessage());
            throw new DPUException(e.getMessage(), e);
        } finally {
            try {
                if(rdfDirectory != null) {
                    FileUtils.deleteDirectory(rdfDirectory);
                }
            } catch (IOException e) {
                LOG.error("Error", e);
                context.sendMessage(MessageType.ERROR, e.getMessage());
                throw new DPUException(e.getMessage(), e);
            }

        }
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
            harvester = new OrganizationsDatanestHarvester(workingDirDpu.getAbsolutePath());
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
                if (tmpRdf.exists()) {
                    String path = tmpRdf.toURI().toURL().toExternalForm();
                    LOG.debug("rdf file: " + path);
                    rdfDataUnit.extractFromFile(extractType, format, path, fileSuffix, baseURI, onlyThisSuffix, handlerExtractType);
                }
            } catch (RDFException e) {
                LOG.error("An error occoured when export was performing. A file: " + tmpRdf.getAbsolutePath(), e);
                context.sendMessage(MessageType.ERROR, e.getMessage());
            } catch (MalformedURLException e) {
                LOG.error("An error occoured when export was performing. A file: " + tmpRdf.getAbsolutePath(), e);
                context.sendMessage(MessageType.ERROR, e.getMessage());
            }
            long triplesCount = rdfDataUnit.getTripleCount();
            LOG.info("A harvesting is successfully finished : " + triplesCount);
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
    public AbstractConfigDialog<CsvOrganizationExtractorConfig> getConfigurationDialog() {
        return new CsvOrganizationExtractorDialog();
    }

}
