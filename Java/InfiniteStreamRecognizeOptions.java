


import org.apache.commons.cli.*;

public class InfiniteStreamRecognizeOptions {
	  String langCode = "ko-KR"; 

	  /** Construct an InfiniteStreamRecognizeOptions class from command line flags. */
	  public static InfiniteStreamRecognizeOptions fromFlags(String[] args) {
	    Options options = new Options();
	    options.addOption(
	        Option.builder()
	            .type(String.class)
	            .longOpt("lang_code")
	            .hasArg()
	            .desc("Language code")
	            .build());

	    CommandLineParser parser = new DefaultParser();
	    CommandLine commandLine;
	    try {
	      commandLine = parser.parse(options, args);
	      InfiniteStreamRecognizeOptions res = new InfiniteStreamRecognizeOptions();

	      if (commandLine.hasOption("lang_code")) {
	        res.langCode = commandLine.getOptionValue("lang_code");
	      }
	      return res;
	    } catch (ParseException e) {
	      System.err.println(e.getMessage());
	      return null;
	    }
	  }
}
