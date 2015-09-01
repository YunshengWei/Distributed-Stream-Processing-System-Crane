import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Grep {
    public static final Options OPTIONS = Grep.buildGrepOptions();
    
    private final OutputStream os;
    private final Pattern pattern;
    private final boolean invertMatch;
    private final boolean count;
    private final List<String> fileList;
    
    /**
     * Print help message of Grep to stdout.
     */
    public static void printHelp() {
        //TODO
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("grep", Grep.OPTIONS);
    }
    
    /**
     * Print usage of Grep to Stdout.
     */
    public static void printUsage() {
        //TODO
    }
    
    private static Options buildGrepOptions() {
        Options options = new Options();
        Grep.addBooleanOptions(options);
        Grep.addArgumentOptions(options);
        return options;
    }
    
    private static void addBooleanOptions(Options options) {
        options.addOption("a", "text", false, 
                "Treat all files as ASCII text.  Normally grep will simply print "
              + "``Binary file ... matches'' if files contain binary characters.  "
              + "Use of this option forces grep to output lines matching the spec"
              + "ified pattern.");
        options.addOption("b", "byte-offset", false,
                "The offset in bytes of a matched pattern is displayed in front of "
              + "the respective matched line.");
        options.addOption("c", "count", false,
                "Only a count of selected lines is written to standard output.");
        options.addOption("E", "extended-regexp", false,
                "Interpret <pattern> as an extended regular expression (i.e. force "
              + "grep to behave as egrep).");
        options.addOption(null, "exclude", false,
                "If specified, it excludes files matching the given filename pat"
              + "tern from the search.  Note that --exclude patterns take priority "
              + "over --include patterns, and if no --include pattern is speci"
              + "fied, all files are searched that are not excluded.  Patterns are"
              + "matched to the full path specified, not only to the filename com"
              + "ponent.");
        options.addOption(null, "exclude-dir", false,
                "If -R is specified, it excludes directories matching the given "
              + "filename pattern from the search.  Note that --exclude-dir pat"
              + "terns take priority over --include-dir patterns, and if no"
              + "--include-dir pattern is specified, all directories are searched "
              + "that are not excluded.");
        options.addOption("F", "fixed-strings", false,
                "Interpret <pattern> as a set of fixed strings (i.e. force grep to "
              + "behave as fgrep).");
        options.addOption("G", "basic-regexp", false,
                "Interpret <pattern> as a basic regular expression (i.e. force grep "
              + "to behave as traditional grep).");
        options.addOption("H", false, 
                "Always print filename headers with output lines.");
        options.addOption("h", "no-filename", false,
                "Never print filename headers (i.e. filenames) with output lines.");
        options.addOption("help", false, "Print a brief help message.");
        options.addOption("I", false, 
                "Ignore binary files.  This option is equivalent to "
              + "--binary-file=without-match option.");
        options.addOption("i", "ignore-case", false,
                "Perform case insensitive matching.  By default, grep is case sen"
              + "sitive.");
        options.addOption(null, "include", false,
                "If specified, only files matching the given filename pattern are "
              + "searched.  Note that --exclude patterns take priority over "
              + "--include patterns.  Patterns are matched to the full path speci"
              + "fied, not only to the filename component.");
        options.addOption(null, "include-dir", false,
                "If -R is specified, only directories matching the given filename "
              + "pattern are searched.  Note that --exclude-dir patterns take pri"
              + "ority over --include-dir patterns.");
        options.addOption("L", "files-without-match", false,
                "Only the names of files not containing selected lines are written "
              + "to standard output.  Pathnames are listed once per file searched.  "
              + "If the standard input is searched, the string ``(standard "
              + "input)'' is written.");
        options.addOption("l", "files-with-matches", false,
                "Only the names of files containing selected lines are written to "
              + "standard output.  grep will only search a file until a match has "
              + "been found, making searches potentially less expensive.  Path "
              + "names are listed once per file searched.  If the standard input "
              + "is searched, the string ``(standard input)'' is written.");
        options.addOption("n", "line-number", false,
                "Each output line is preceded by its relative line number in the "
              + "file, starting at line 1.  The line number counter is reset for "
              + "each file processed.  This option is ignored if -c, -L, -l, or -q "
              + "is specified.");
        options.addOption(null, "null", false,
                "Prints a zero-byte after the file name.");
        options.addOption("O", false, 
                "If -R is specified, follow symbolic links only if they were "
              + "explicitly listed on the command line.  The default is not to "
              + "follow symbolic links.");
        options.addOption("o", "only-matching", false, 
                "Prints only the matching part of the lines.");
        options.addOption("p", false,
                "If -R is specified, no symbolic links are followed.  This is the "
              + "default.");
        options.addOption("q", "quiet", false,
                "Quiet mode: suppress normal output.  grep will only search a file "
              + "until a match has been found, making searches potentially less "
              + "expensive.");
        options.addOption(null, "silent", false, "See --quiet.");
        options.addOption("r", "recursive", false,
                "Recursively search subdirectories listed.");
        options.addOption("R", false, "See -r.");
        options.addOption("S", false,
                "If -R is specified, all symbolic links are followed.  The default "
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
                "The expression is searched for as a word (as if surrounded by "
              + "`[[:<:]]' and `[[:>:]]'; see re_format(7)).");
        options.addOption("x", "line-regexp", false,
                "Only input lines selected against an entire fixed string or regu"
              + "lar expression are considered to be matching lines.");
        options.addOption("Z", "decompress", false,
                "Force grep to behave as zgrep.");
        options.addOption("z", false, "See -Z.");
        options.addOption(null, "line-buffered", false,
                "Force output to be line buffered.  By default, output is line "
              + "buffered when standard output is a terminal and block buffered"
              + "otherwise.");
    }
    
    private static void addArgumentOptions(Options options) {
        options.addOption(Option.builder("A").longOpt("after-context")
                .hasArg().argName("num").desc(
                        "Print <num> lines of trailing context after each match.  See "
                      + "also the -B and -C options.").build());
        options.addOption(Option.builder("B").longOpt("before-context")
                .hasArg().argName("num").desc(
                        "Print <num> lines of leading context before each match.  See "
                      + "also the -A and -C options.").build());
        options.addOption(Option.builder("C").longOpt("context")
                .hasArg().argName("num").optionalArg(true).desc(
                        "Print <num> lines of leading and trailing context surrounding "
                      + "each match.  The default is 2 and is equivalent to -A 2 -B "
                      + "2.  Note: no whitespace may be given between the option and"
                      + "its argument.").build());
        options.addOption(Option.builder("d").longOpt("directories")
                .hasArg().argName("action").desc(
                        "Specify the demanded action for directories.  It is `read' "
                      + "by default, which means that the directories are read in the "
                      + "same manner as normal files.  Other possible values are "
                      + "`skip' to silently ignore the directories, and `recurse' to "
                      + "read them recursively, which has the same effect as the -R "
                      + "and -r option.").build());
        options.addOption(Option.builder("e").longOpt("regexp")
                .hasArg().argName("pattern").desc(
                        "Specify a pattern used during the search of the input: an "
                      + "input line is selected if it matches any of the specified "
                      + "patterns.  This option is most useful when multiple -e "
                      + "options are used to specify multiple patterns, or when a "
                      + "pattern begins with a dash (`-').").build());
        options.addOption(Option.builder("m").longOpt("max-count")
                .hasArg().argName("num").desc(
                        "Stop reading the file after <num> matches.").build());
    }
    
    /**
     * Construct Grep from specified options and will output matched lines to
     * the specified output.
     * 
     * @param args
     *            options for Grep.
     * @param os
     *            the OutputStream to which matched lines should be write to.
     * @throws ParseException
     *             if any of the specified options is not valid.
     */
    public Grep(String[] args, OutputStream os) throws ParseException {
        this.os = os;
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(Grep.OPTIONS, args);
        List<String> argList = cmd.getArgList();
        
        String[] regexps = cmd.getOptionValues("e");
        String regexp;
        if (regexps != null) {
            regexp = ".*(" + String.join("|", regexps) + ").*";
            this.fileList = argList;
        } else {
            regexp = ".*(" + argList.get(0) + ").*";
            this.fileList = argList.subList(1, argList.size());
        }
        
        if (cmd.hasOption("ignore-case")) {
            this.pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
        } else {
            this.pattern = Pattern.compile(regexp);
        }
        
        this.invertMatch = cmd.hasOption("invert-match");
        this.count = cmd.hasOption("count");
    }
    
    private void grep(BufferedReader br, PrintWriter pw, String prefix) {
        String line = null;
        try {
            int countMatches = 0;
            while ((line = br.readLine()) != null) {
                if (pattern.matcher(line).matches() ^ invertMatch) {
                    countMatches += 1;
                    if (!this.count) {
                        pw.println(prefix + line);
                    }
                }
            }
            
            if (this.count) {
                pw.println(prefix + countMatches);
            }
        } catch (IOException e) {
            System.err.println(prefix + " Error occurs when reading.");
        }
    }
    
    public void execute() {
        PrintWriter pw = new PrintWriter(os, true);
        BufferedReader br;
        
        if (fileList.isEmpty()) {
            br = new BufferedReader(new InputStreamReader(System.in));
            grep(br, pw, "");
        } else {
            for (String fileName : fileList) {
                String prefix = fileName + ":";
                try {
                    br = new BufferedReader(new FileReader(fileName));
                    grep(br, pw, prefix);
                    br.close();
                } catch (FileNotFoundException e) {
                    System.err.println(prefix + " No such file.");
                } catch (IOException e) {
                    System.err.println(prefix + " Error occurs when closing.");
                }
            }
        }
    }
    
    // Unit test
    public static void main(String args[]) {
        try {
            Grep grep = new Grep(args, System.out);
            grep.execute();
        } catch (ParseException e) {
            Grep.printHelp();
        }
    }
}
