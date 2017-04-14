package hulva.luva.tools.escloner.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hulva Luva.H from ECBD
 * @date 2017年4月13日
 * @description
 *
 */
public class CommandLineUtil {
    protected final static Logger LOGGER = LoggerFactory.getLogger(CommandLineUtil.class);

    /**
     * Builds a commandLine key pair values from an array of strings
     *
     * @param args the command line arguments
     * @return the list of atomic option and value tokens
     * @throws ParseException if unable to parse any of the given arguments against the available
     *         options
     */
    public static CommandLine readCommandLine(String[] args) throws ParseException {
        CommandLine cmd = null;
        CommandLineParser parser = new DefaultParser();
        Options options = createOptions();
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.error("Unable to parse given arguments against the available options.", e);
            help(options);
        }
        return cmd;
    }

    public static Options createOptions() {
        Options options = new Options();
        options.addOption(Option.builder("sServer").hasArg()
                .desc("source: host:port (e.g. localhost:9300)").required().build());
        options.addOption(
                Option.builder("index").hasArg().desc("source: index name").required().build());
        options.addOption(Option.builder("dServer").hasArg()
                .desc("destination: host:port (e.g. localhost:9300)").required().build());
        options.addOption(Option.builder("template").hasArg().desc("template: index template name")
                .required().build());
        options.addOption(Option.builder("pSize").hasArg()
                .desc("pSize: how many thread to put document to destination server").required()
                .build());
        return options;
    }

    private static void help(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar escloner.jar", options);
        System.exit(0);// TODO: review: maybe we can throw an uncaught exceptions which can bubble
                       // up instead of a kill!
    }
}
