public class ApplicationConfig {
    private static final ApplicationConfig INSTANCE = new ApplicationConfig();
    
    private final String corpusUrl;
    private final String featuresNum;
    //private final String dbPassword;
    private final boolean locally =true; //false if want to run on aws 
    
    private ApplicationConfig() {
        // Load from file, environment variables, or hardcode
        if (locally){

        }
        else{
            
        }
        this.corpusUrl = "";
        this.featuresNum = "";
        //this.dbPassword = "";
    }
    
    public static ApplicationConfig getInstance() {
        return INSTANCE;
    }
    
    // Getters
    public String getcorpusUrl() { return corpusUrl; }
    public String getfeaturesNum() { return featuresNum; }
    //public String getDbPassword() { return dbPassword; }
}