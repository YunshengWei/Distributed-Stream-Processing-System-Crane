import java.io.OutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Grep {
    public static final Options OPTIONS = Grep.buildGrepOptions();
    
    private static Options buildGrepOptions() {
        Options options = new Options();
        Grep.addBooleanOptions(options);
        return options;
    }
    
    private static void addBooleanOptions(Options options) {
        options.addOption("a", "text", false, 
                "Treat all files as ASCII text.  Normally grep will simply print"
              + "``Binary file ... matches'' if files contain binary characters."
              + "Use of this option forces grep to output lines matching the spec"
              + "ified pattern.");
        options.addOption("b", "byte-offset", false,
                "The offset in bytes of a matched pattern is displayed in front of"
              + "the respective matched line.");
        options.addOption("c", "count", false,
                "Only a count of selected lines is written to standard output.");
        options.addOption("E", "extended-regexp", false,
                "Interpret pattern as an extended regular expression (i.e. force"
              + "grep to behave as egrep).");
        options.addOption(null, "exclude", false,
                "If specified, it excludes files matching the given filename pat"
              + "tern from the search.  Note that --exclude patterns take priority"
              + "over --include patterns, and if no --include pattern is speci"
              + "fied, all files are searched that are not excluded.  Patterns are"
              + "matched to the full path specified, not only to the filename com"
              + "ponent.");
        options.addOption(null, "exclude-dir", false,
                "If -R is specified, it excludes directories matching the given"
              + "filename pattern from the search.  Note that --exclude-dir pat"
              + "terns take priority over --include-dir patterns, and if no"
              + "--include-dir pattern is specified, all directories are searched"
              + "that are not excluded.");
        options.addOption("F", "fixed-strings", false,
                "Interpret pattern as a set of fixed strings (i.e. force grep to"
              + "behave as fgrep).");
        options.addOption("G", "basic-regexp", false,
                "Interpret pattern as a basic regular expression (i.e. force grep"
              + "to behave as traditional grep).");
        options.addOption("H", false, 
                "Always print filename headers with output lines.");
        options.addOption("h", "no-filename", false,
                "Never print filename headers (i.e. filenames) with output lines.");
        options.addOption("help", false, "Print a brief help message.");
        options.addOption("I", false, 
                "Ignore binary files.  This option is equivalent to"
              + "--binary-file=without-match option.");
        options.addOption("i", "ignore-case", false,
                "Perform case insensitive matching.  By default, grep is case sen"
              + "sitive.");
        options.addOption(null, "include", false,
                "If specified, only files matching the given filename pattern are"
              + "searched.  Note that --exclude patterns take priority over"
              + "--include patterns.  Patterns are matched to the full path speci"
              + "fied, not only to the filename component.");
        options.addOption(null, "include-dir", false,
                "If -R is specified, only directories matching the given filename"
              + "pattern are searched.  Note that --exclude-dir patterns take pri"
              + "ority over --include-dir patterns.");
        options.addOption("J", "bz2decompress", false,
                "Decompress the bzip2(1) compressed file before looking for the"
              + "text.");
        options.addOption("L", "files-without-match", false,
                "Only the names of files not containing selected lines are written"
              + "to standard output.  Pathnames are listed once per file searched."
              + "If the standard input is searched, the string ``(standard"
              + "input)'' is written.");
        options.addOption("l", "files-with-matches", false,
                "Only the names of files containing selected lines are written to"
              + "standard output.  grep will only search a file until a match has"
              + "been found, making searches potentially less expensive.  Path"
              + "names are listed once per file searched.  If the standard input"
              + "is searched, the string ``(standard input)'' is written.");
        options.addOption(null, "mmap", false,
                "Use mmap(2) instead of read(2) to read input, which can result in"
              + "better performance under some circumstances but can cause unde"
              + "fined behaviour.");
        options.addOption("n", "line-number", false,
                "Each output line is preceded by its relative line number in the"
              + "file, starting at line 1.  The line number counter is reset for"
              + "each file processed.  This option is ignored if -c, -L, -l, or -q"
              + "is specified.");
        options.addOption(null, "null", false,
                "Prints a zero-byte after the file name.");
        options.addOption("O", false, 
                "If -R is specified, follow symbolic links only if they were"
              + "explicitly listed on the command line.  The default is not to"
              + "follow symbolic links.");
        options.addOption("o", "only-matching", false, 
                "Prints only the matching part of the lines.");
        options.addOption("p", false,
                "If -R is specified, no symbolic links are followed.  This is the"
              + "default.");
        options.addOption("q", "quiet", false,
                "Quiet mode: suppress normal output.  grep will only search a file"
              + "until a match has been found, making searches potentially less"
              + "expensive.");
        options.addOption(null, "silent", false, "See --quiet.");
        options.addOption("r", "recursive", false,
                "Recursively search subdirectories listed.");
        options.addOption("R", false, "See -r.");
        options.addOption("S", false,
                "If -R is specified, all symbolic links are followed.  The default"
              + "is not to follow symbolic links.");
        options.addOption("s", "no-messages", false,
                "Silent mode.  Nonexistent and unreadable files are ignored (i.e. "
              + "their error messages are suppressed).");
        options.addOption("U", "binary", false, 
                "Search binary files, but do not attempt to print them.");
        options.addOption("V", "version", false,
                "Display version information and exit.");
        options.addOption("v", "invert-match", false,
                "Selected lines are those not matching any of the specified pat"
              + "terns.");
        options.addOption("w", "word-regexp", false,
                "The expression is searched for as a word (as if surrounded by"
              + "`[[:<:]]' and `[[:>:]]'; see re_format(7)).");
        options.addOption("x", "line-regexp", false,
                "Only input lines selected against an entire fixed string or regu-"
              + "lar expression are considered to be matching lines.");
        options.addOption("Z", "decompress", false,
                "Force grep to behave as zgrep.");
        options.addOption("z", false, "See -Z.");
        options.addOption(null, "line-buffered", false,
                "Force output to be line buffered.  By default, output is line"
              + "buffered when standard output is a terminal and block buffered"
              + "otherwise.");
    }
    
    public Grep(String[] args, OutputStream os) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(Grep.OPTIONS, args);
    }
    
    public void grep() {
    }
    
    // Unit test
    public static void main(String args[]) throws ParseException {
        //Grep grep = new Grep(args, System.out);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("grep", Grep.OPTIONS);
        
    }
}
