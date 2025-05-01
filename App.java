import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonElasticMapReduce emr;

    // S3 bucket path – ensure that this bucket exists in your AWS account.
    public static String bucket = "s3://ass3emb3151";  // Replace with your actual bucket name.
    
    // Input paths:
    public static String syntacticInput = bucket + "/corpus10";
    public static String goldStandardInput = bucket + "/data/word-relatedness.txt";
    
    // Output paths for each step:
    public static String step1Output    = bucket + "/output/step1/";    // Output of Step1: co-occurrence vectors
    public static String step1_5Output  = bucket + "/output/step1_5/";  // Output of Step1_5: global feature counts
    public static String step2Output    = bucket + "/output/step2/";    // Output of Step2: association measures
    public static String step3Output    = bucket + "/output/step3/";    // Output of Step3: vector similarities
    public static String step4Output    = bucket + "/output/step4/";    // Output of Step4: classification/evaluation
    public static String SortFeaturesop    = bucket + "/output/SortFeatures/";

    // JAR paths for each job – these jars must be built and uploaded to S3.
    public static String jarStep1    = bucket + "/jars/Step1.jar";     // For building co-occurrence vectors
    public static String jarStep1_5  = bucket + "/jars/Step1_5.jar";   // For counting features (new step)
    public static String jarStep2    = bucket + "/jars/Step2.jar";     // For computing association measures
    public static String jarStep3    = bucket + "/jars/Step3.jar";     // For computing vector similarities
    public static String jarStep4    = bucket + "/jars/Step4.jar";     // For classification/evaluation
    public static String jarSortFeatures    = bucket + "/jars/SortFeatures.jar"; 

    public static void main(String[] args) {
        // Initialize AWS credentials and the EMR client.
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to AWS EMR...");
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        // Debug: print existing clusters
        System.out.println("[INFO] Existing clusters:");
        System.out.println(emr.listClusters());

        // --- Define the workflow steps ---

        // Step 1: Build co-occurrence vectors (HashMap stripes)
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar(jarStep1)
                .withMainClass("Step1")
                .withArgs(syntacticInput, step1Output);
        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1: Build co-occurrence vectors")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 1.5: Count raw feature occurrences from Step1 output
        HadoopJarStepConfig step1_5 = new HadoopJarStepConfig()
                .withJar(jarStep1_5)
                .withMainClass("Step1_5")
                .withArgs(step1Output, step1_5Output);
        StepConfig stepConfig1_5 = new StepConfig()
                .withName("Step1.5: Count feature occurrences")
                .withHadoopJarStep(step1_5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step SortFeatures: gets top 1000 features
        HadoopJarStepConfig SortFeatures = new HadoopJarStepConfig()
                .withJar(jarSortFeatures)
                .withMainClass("SortFeatures")
                .withArgs(step1_5Output, SortFeaturesop);
        StepConfig stepConfigSortFeatures = new StepConfig()
                .withName("Step: SortFeatures")
                .withHadoopJarStep(SortFeatures)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2: Compute association measures using Step1_5's totals
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar(jarStep2)
                .withMainClass("Step2")
                // Note: Step2 now reads input from Step1_5 output to get correct global totals.
                .withArgs(step1Output, SortFeaturesop, step2Output);
        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2: Compute association measures")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 3: Merge hash tables and compute vector similarities
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar(jarStep3)
                .withMainClass("Step3")
                .withArgs(step2Output, step3Output);
        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3: Compute vector similarities")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 4: Classification and evaluation.
        HadoopJarStepConfig step4 = new HadoopJarStepConfig()
                .withJar(jarStep4)
                .withMainClass("Step4")
                .withArgs(goldStandardInput, step3Output, step4Output);
        StepConfig stepConfig4 = new StepConfig()
                .withName("Step4: Classification and Evaluation")
                .withHadoopJarStep(step4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // --- Create the list of steps ---
        StepConfig[] steps = new StepConfig[] { /*stepConfig1, stepConfig1_5,  stepConfigSortFeatures,*/stepConfig2, stepConfig3, stepConfig4 };

        // --- Configure the EMR cluster ---
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(6)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")  // Replace with your actual EC2 key name.
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        // --- Create and run the EMR job flow ---
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Semantic Similarity MapReduce Workflow (5-step)")
                .withInstances(instances)
                .withSteps(steps)
                .withLogUri(bucket + "/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        System.out.println("[INFO] Submitting EMR job flow...");
        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("[INFO] EMR job flow submitted. Job Flow ID: " + jobFlowId);
    }
}
