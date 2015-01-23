package org.cvrgrid.waveform;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.github.jmchilton.blend4j.galaxy.GalaxyInstance;
import com.github.jmchilton.blend4j.galaxy.GalaxyInstanceFactory;
import com.github.jmchilton.blend4j.galaxy.GalaxyResponseException;
import com.github.jmchilton.blend4j.galaxy.HistoriesClient;
import com.github.jmchilton.blend4j.galaxy.WorkflowsClient;
import com.github.jmchilton.blend4j.galaxy.beans.History;
import com.github.jmchilton.blend4j.galaxy.beans.Workflow;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs.WorkflowDestination;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowOutputs;
import com.sun.jersey.api.client.ClientResponse;



public class RunGalaxyWorkflow {
	public static void main(final String[] args) throws Exception {
		String instanceUrl = "http://ec2-54-211-1-192.compute-1.amazonaws.com:8081/";
		String apiKey = "ed6d88d896b8d770254c6ab8e6abe201";
		String workFlowName = "Copy of CVRG - sqrs with data transfer";
//		final String workFlowTemplate = "ZZZZZZZZZZ";
		String historyName = "test103";
	    String fileName = "3793588_0035";
	    String filePath = "~/matched/s00402/";
	    String globusEndpoint  = "cvrgglobusproc#gcmu-02";
	    
	    RunGalaxyWorkflow rGW = new RunGalaxyWorkflow();
	    rGW.runGalaxyWF(instanceUrl, apiKey, workFlowName, historyName, fileName, filePath, globusEndpoint);
//		runGalaxyWFHack(instanceUrl, apiKey, workFlowTemplate, historyName, fileName, filePath, globusEndpoint);
		
		String[][]recordArray = { 
				{"~/matched/s00402/","3793588_0001"}, 
				{"~/matched/s00402/","3793588_0002"}, 
				{"~/matched/s00402/","3793588_0003"}, 
				{"~/matched/s00402/","3793588_0004"}, 
				{"~/matched/s00402/","3793588_0005"}, 
				{"~/matched/s00402/","3793588_0006"}, 
				{"~/matched/s00402/","3793588_0007"}, 
				{"~/matched/s00402/","3793588_0008"}, 
				{"~/matched/s00402/","3793588_0009"}, 
				{"~/matched/s00402/","3793588_0010"}, 
				{"~/matched/s00402/","3793588_0011"}, 
				{"~/matched/s00402/","3793588_0012"}, 
				{"~/matched/s00402/","3793588_0013"}, 
				{"~/matched/s00402/","3793588_0014"}, 
				{"~/matched/s00402/","3793588_0015"}, 
				{"~/matched/s00402/","3793588_0016"}, 
				{"~/matched/s00402/","3793588_0017"}, 
				{"~/matched/s00402/","3793588_0018"}, 
				{"~/matched/s00402/","3793588_0019"}, 
				{"~/matched/s00402/","3793588_0020"}, 
				{"~/matched/s00402/","3793588_0021"}, 
				{"~/matched/s00402/","3793588_0022"}, 
				{"~/matched/s00402/","3793588_0023"}, 
				{"~/matched/s00402/","3793588_0024"}, 
				{"~/matched/s00402/","3793588_0025"}, 
				{"~/matched/s00402/","3793588_0026"}, 
				{"~/matched/s00402/","3793588_0027"}, 
				{"~/matched/s00402/","3793588_0028"}, 
				{"~/matched/s00402/","3793588_0029"}, 
				{"~/matched/s00402/","3793588_0030"}, 
				{"~/matched/s00402/","3793588_0031"}, 
				{"~/matched/s00402/","3793588_0032"}, 
				{"~/matched/s00402/","3793588_0033"}, 
				{"~/matched/s00402/","3793588_0034"}, 
				{"~/matched/s00402/","3793588_0035"} 
			};
//		runGalaxyWF2Multiple(instanceUrl, apiKey, workFlowName, historyName, recordArray, globusEndpoint);
		
	}

	  /** Creates and runs a Galaxy Workflow based on the workFlowTemplate.<BR>
	   * Replaces the placeholder text (XXXXXXXXXX and YYYYYYYYYY) in the template workflow with fileName and FilePath.<BR>
	   * This is a kludge to the go with the hack required to execute a workflow using blend4j library which doesn't accept parameters.<BR>
	   * 
	   * @param instanceUrl - URL of the Galaxy instance run the workflow on.
	   * @param apiKey - the Galaxy API key for a user who can access the templateWorkflow.
	   * @param workFlowTemplate - the name of workflow template e.g. "ZZZZZZZZZZ"
	   * @param workflowJSON - JSON representation of the (sqrs) template workflow.
	   * @param historyName - name of an existing history to record the execution of new workflow in.
	   * @param recordName - WFDB record name (e.g. "3793588_0035") assumed to be the filename of the ".hea" file and of a single ".dat" file.
	   * @param filePath - Relative location of the WFDB record on Globus Server (e.g. "~/matched/s00402/").
	   * @param globusEndpoint - name of the Globus endpoint containing the WFDB record files (e.g. "cvrgglobusproc#gcmu-02")
	   * @return
	   */
	  public void runGalaxyWFHack(final String instanceUrl, final String apiKey, final String workFlowTemplate, String historyName,
			  String fileName, String filePath, String globusEndpoint) {
		    final GalaxyInstance instance = GalaxyInstanceFactory.get(instanceUrl, apiKey);
		    final WorkflowsClient workflowsClient = instance.getWorkflowsClient();

		    // Find history
		    System.out.println("Find history: ");
		    final HistoriesClient historyClient = instance.getHistoriesClient();
		    History matchingHistory = null;
		    for(final History history : historyClient.getHistories()) {
		      if(history.getName().equals(historyName)) { // blend4j Test History // TestHistory1 
		        matchingHistory = history;
		        System.out.println(history.getId() + ") " + history.getName());
		      }
		    }

		    System.out.println("Find workflow: ");
		    Workflow matchingWorkflow = null;
		    for(Workflow workflow : workflowsClient.getWorkflows()) {
		      if(workflow.getName().equals(workFlowTemplate)) {
		        matchingWorkflow = workflow;
		        System.out.println(workflow.getId() + ") " + workflow.getName());
		      }
		    }

		    String workflowJSON = workflowsClient.exportWorkflow(matchingWorkflow.getId());	    
			 // modify workflow json
		    workflowJSON = replaceFileNameJSON(workflowJSON, fileName, filePath, globusEndpoint);
//		    System.out.println("JSON-encoded representation of the Workflow:\n" + workflowJSON);
		    Workflow importedWorkflow = workflowsClient.importWorkflow(workflowJSON);
		    String importedWorkflowId = importedWorkflow.getId();
		    
		    final WorkflowInputs inputs = new WorkflowInputs();
		    inputs.setDestination(new WorkflowInputs.ExistingHistory(matchingHistory.getId()));
		    inputs.setWorkflowId(importedWorkflowId);
		   
		    final WorkflowOutputs output = workflowsClient.runWorkflow(inputs);
		    System.out.println("Running workflow in history " + output.getHistoryId());
		    for(String outputId : output.getOutputIds()) {
		      System.out.println("  Workflow writing to output id " + outputId);
		    }
		  }
	  
	  
	  /** Replaces the placeholder text (XXXXXXXXXX and YYYYYYYYYY) in the template workflow ZZZZZZZZZZ with fileName and FilePath.<BR>
	   * This is a kludge to the go with the hack required to execute a workflow using blend4j library which doesn't accept parameters.<BR>
	   * It is not efficient, and will probably still run faster than the workflow itself.
	   * 
	   * @param workflowJSON - JSON representation of the (sqrs) template workflow.
	   * @param recordName - WFDB record name (e.g. "3793588_0035") assumed to be the filename of the ".hea" file and of a single ".dat" file.
	   * @param filePath - Relative location of the WFDB record on Globus Server (e.g. "~/matched/s00402/").
	   * @param globusEndpoint - name of the Globus endpoint containing the WFDB record files (e.g. "cvrgglobusproc#gcmu-02")
	   * @return
	   */
	  private String replaceFileNameJSON(String workflowJSON, String recordName, String filePath, String globusEndpoint){
		  Date date = new Date();
	      SimpleDateFormat ft = new SimpleDateFormat ("'SQRS_" + recordName + "' yyyy.MM.dd  hh:mm:ss a");
		  String workflowName = ft.format(date);
		  
		  workflowJSON = workflowJSON.replace("WWWWWWWWWW", globusEndpoint); // e.g. "cvrgglobusproc#gcmu-02"
		  workflowJSON = workflowJSON.replace("XXXXXXXXXX", recordName); // e.g. "3793588_0035"
		  workflowJSON = workflowJSON.replace("YYYYYYYYYY", filePath);	// e.g. "~/matched/s00402/"
		  workflowJSON = workflowJSON.replace("ZZZZZZZZZZ", workflowName);	  
		  
		  return workflowJSON;	  
	  }
	  
	  
	  /** Runs an existing Galaxy Workflow .<BR>
	   * Passes it the input and output fileNames and FilePaths.<BR>
	   * 
	   * @param instanceUrl - URL of the Galaxy instance run the workflow on.
	   * @param apiKey - the Galaxy API key for a user who can access the templateWorkflow.
	   * @param workFlowName - the name of workflow e.g. "Copy of CVRG - sqrs with data transfer"  
	   * @param historyName - name of an existing history to record the execution of new workflow in.
	   * @param recordName - WFDB record name (e.g. "3793588_0035") assumed to be the filename of the ".hea" file and of a single ".dat" file.
	   * @param filePath - Relative location of the WFDB record on Globus Server (e.g. "~/matched/s00402/").
	   * @param globusEndpoint - name of the Globus endpoint containing the WFDB record files (e.g. "cvrgglobusproc#gcmu-02")
	   * @return
	   */
	  public void runGalaxyWF(final String instanceUrl, final String apiKey, final String workFlowName, String historyName,
			  String recordName, String filePath, String globusEndpoint) {
		    final GalaxyInstance instance = GalaxyInstanceFactory.get(instanceUrl, apiKey);
		    final WorkflowsClient workflowsClient = instance.getWorkflowsClient();		    

		    System.out.println("Find workflow: ");
		    Workflow matchingWorkflow = null;
		    for(Workflow workflow : workflowsClient.getWorkflows()) {
//		      System.out.println(workflow.getId() + ") " + workflow.getName());
		      if(workflow.getName().equals(workFlowName)) { //"Copy of CVRG - sqrs with data transfer"  
		        matchingWorkflow = workflow;		        
		        break;
		      }
		    }
		    System.out.println("found id:" + matchingWorkflow.getId());

		    Map<String,Object>[] hea = buildParameterArray(filePath + recordName + ".hea", "from_path");
		    Map<String,Object>[] dat = buildParameterArray(filePath + recordName + ".dat", "from_path");
		    Map<String,Object>[] sqrs =buildParameterArray(filePath + recordName + "_sqrs.txt","to_path");
		    Map<String,Object>[] wqrs =buildParameterArray(filePath + recordName + "_wqrs.txt","to_path");
		    
			Date date = new Date();
			SimpleDateFormat ft = new SimpleDateFormat ("'" + historyName + "-" + recordName + "' yyyy.MM.dd  hh:mm:ss a");
			historyName = ft.format(date);
   
		    final WorkflowInputs inputs = new WorkflowInputs();
		    inputs.setDestination(new WorkflowInputs.NewHistory(historyName) );
		    inputs.setWorkflowId(matchingWorkflow.getId());
		    // setStepParameter(String stepId, String parameterName, Object parameterValue) 
		    // Adds an element to the Java Map WorkflowInputs.parameters, 
		    // which when it arrives at Galaxy's Python backend will be a Python value in the following form:
		    //------------------
			//		    'parameters': 
			//			{
			//				'<stepId>': 
			//					{'<parameterName>': 
			//						<parameterValue> 
			//					}, 
			//				'step495': {'globus_send_data': [{'value': '~/matched/s00402/3793588_0035_wqrs.txt', 'param': 'to_path'}]}, 
			//				'step487': {'globus_get_data': [{'value': '~/matched/s00402/3793588_0035.hea', 'param': 'from_path'}]}, 
			//				'step486': {'globus_get_data': [{'value': '~/matched/s00402/3793588_0035.dat', 'param': 'from_path'}]}
			//			}, 
		    // -----------------
		    // parameterValue is expected to be a single element Hashmap Array, with the element being a Hashmap with the keys "value" and "param".
		    // which yields the following Python value at Galaxy:
		    // [{'value': '<valueString>', 'param': '<paramString>'}]
		    inputs.setStepParameter("step" + 487, "globus_get_data", hea);
		    inputs.setStepParameter("step" + 486, "globus_get_data", dat);
		    inputs.setStepParameter("step" + 495, "globus_send_data", sqrs);
		    inputs.setStepParameter("step" + 494, "globus_send_data", wqrs);
		    
		    final ClientResponse output1 = workflowsClient.runWorkflowResponse(inputs);
		    System.out.println("clientReponse:" + output1.getClientResponseStatus());
		   
//		    final WorkflowOutputs output = workflowsClient.runWorkflow(inputs);
//		    System.out.println("Running workflow in history " + output.getHistoryId());
//		    for(String outputId : output.getOutputIds()) {
//		      System.out.println("  Workflow writing to output id " + outputId);
//		    }
		  }
	  
	  /** Builds the Java array of Maps object, which when it arrives at Galaxy's Python backend will be a Python value in the following form:
		[
			{	
			'value': '~/matched/s00402/3793588_0035_sqrs.txt', 
			'param': 'to_path'
			}
		]
	   *  
	   * @param value - Value to be passed to Galaxy Workflow which expects a "Set at Runtime" parameter. Omit the single quotes, they will be added by the API  e.g.  ~/matched/s00402/3793588_0035_sqrs.txt
	   * @param paramName - Name of the "Set at Runtime" parameter. Omit the single quotes. e.g.  to_path
	   * @return an array with a single Map in it.
	   */
	  private Map<String, Object>[] buildParameterArray(String value, String paramName){
		  Map<String, Object> keyValueMap = new HashMap<String, Object>();
		  keyValueMap.put("value", value);
		  keyValueMap.put("param", paramName);
		  
		  Map<String, Object>[] arrayParam = new HashMap[1];
		  arrayParam[0] = keyValueMap;
		  
		  return arrayParam;	  
	  }

	  
	  /** Runs an existing Galaxy Workflow .<BR>
	   * Passes it the input and output fileNames and FilePaths.<BR>
	   * 
	   * @param instanceUrl - URL of the Galaxy instance run the workflow on.
	   * @param apiKey - the Galaxy API key for a user who can access the templateWorkflow.
	   * @param workFlowName - the name of workflow e.g. "Copy of CVRG - sqrs with data transfer"  
	   * @param historyName - name of an existing history to record the execution of new workflow in.
	   * @param recordName - WFDB record name (e.g. "3793588_0035") assumed to be the filename of the ".hea" file and of a single ".dat" file.
	   * @param filePath - Relative location of the WFDB record on Globus Server (e.g. "~/matched/s00402/").
	   * @param globusEndpoint - name of the Globus endpoint containing the WFDB record files (e.g. "cvrgglobusproc#gcmu-02")
	   * @return
	   */
	  public void runGalaxyWF2Multiple(final String instanceUrl, final String apiKey, final String workFlowName, String historyName,
			  String[][]recordArray, String globusEndpoint) {
		  String recordName="", filePath="";
		  
		  for(String[] record : recordArray){
			  filePath = record[0];
			  recordName = record[1];
			  System.out.println(filePath + " : " + recordName);
			  runGalaxyWF(instanceUrl, apiKey, workFlowName, historyName, recordName, filePath, globusEndpoint);
		  }
		  
	  }

	  /** Gets a destination object describing the Galaxy History.
	   * 
	   * @param instanceUrl - URL of the Galaxy instance run the workflow on.
	   * @param apiKey - the Galaxy API key for a user who can access the templateWorkflow.
	   * @param historyName - name of Galaxy History to record the execution of new workflow in, creating a new one if needed.
	   * @param addDateToHistory - if true, adds date/time to the history name in the form "historyName - yyyy.MM.dd  hh:mm:ss a" and creates a new Galaxy History.<BR>
	   * - if false, searches for an existing history to use, creating an new one if not found.
	   * @return
	   */
	  public WorkflowDestination getDestination(String instanceUrl, String apiKey, String historyName, boolean addDateToHistory){
		  History existingHistory = null;
		  if(addDateToHistory){
			  Date date = new Date();
			  SimpleDateFormat ft = new SimpleDateFormat ("'" + historyName + " -' yyyy.MM.dd  hh:mm:ss a");
			  historyName = ft.format(date);
		  }else{		  	
			  // Find history
			  final GalaxyInstance instance = GalaxyInstanceFactory.get(instanceUrl, apiKey);

//			  System.out.println("Find history: ");
			  final HistoriesClient historyClient = instance.getHistoriesClient();
			  for(final History history : historyClient.getHistories()) {
				  if(history.getName().equals(historyName)) { // blend4j Test History // TestHistory1 
					  existingHistory = history;
//					  System.out.println("Found history: " + history.getId() + ") " + history.getName());
				  }
			  }
		  }

		  WorkflowDestination destination = null;
		  if(existingHistory != null){
			  destination = new WorkflowInputs.ExistingHistory(existingHistory.getId());
		  }else{
			  destination = new WorkflowInputs.NewHistory(historyName);
		  }

		  return destination;
	  }
	  
	  /** Runs an existing Galaxy Workflow, using a named Galaxy History.<BR>
	   * Passes it the input and output fileNames and FilePaths.
	   * 
	   * @param instanceUrl - URL of the Galaxy instance on which to run the workflow.
	   * @param apiKey - the Galaxy API key for a user who can access the templateWorkflow.
	   * @param workFlowName - the name of workflow e.g. "Copy of CVRG - sqrs with data transfer"  
	   * @param historyName - name of Galaxy History to record the execution of new workflow in, creating a new one if needed.
	   * @param addDateToHistory - if true, adds date/time to the history name in the form "historyName - yyyy.MM.dd  hh:mm:ss a" and creates a new Galaxy History.<BR>
	   * - if false, searches for an existing history to use, creating an new one if not found.
	   * @param paramArray - an array of "To be set at runtime" parameters which this Galaxy Workflow requires.
	   * @return
	   */
	  public void runGalaxyWF(final String instanceUrl, final String apiKey, final String workFlowName, 
			  	String historyName, boolean addDateToHistory, galaxyParameter[] paramArray) {

		    WorkflowDestination destination = getDestination(instanceUrl, apiKey, historyName, addDateToHistory);
		    runGalaxyWF(instanceUrl, apiKey, workFlowName, destination, paramArray);
	  }
	  
	  /** Runs an existing Galaxy Workflow, using the WorkflowDestination to specify the Galaxy History to use.<BR>
	   * Passes it the input and output fileNames and FilePaths.<BR>
	   * 
	   * @param instanceUrl - URL of the Galaxy instance on which to run the workflow.
	   * @param apiKey - the Galaxy API key for a user who can access the templateWorkflow.
	   * @param workFlowName - the name of workflow e.g. "Copy of CVRG - sqrs with data transfer"  
	   * @param destination - WorkflowDestination object describing an existing Galaxy History to record the running in.
	   * @param paramArray - an array of "To be set at runtime" parameters which this Galaxy Workflow requires.
	   * @return
	   */
	  public void runGalaxyWF(final String instanceUrl, final String apiKey, final String workFlowName, WorkflowDestination destination, galaxyParameter[] paramArray) {
		  
		    final GalaxyInstance instance = GalaxyInstanceFactory.get(instanceUrl, apiKey);
		    final WorkflowsClient workflowsClient = instance.getWorkflowsClient();		    

//		    System.out.println("Find workflow: ");
		    Workflow matchingWorkflow = null;
		    for(Workflow workflow : workflowsClient.getWorkflows()) {
//		      System.out.println(workflow.getId() + ") " + workflow.getName());
		      if(workflow.getName().equals(workFlowName)) { //"Copy of CVRG - sqrs with data transfer"  
		        matchingWorkflow = workflow;		        
		        break;
		      }
		    }
//		    System.out.println("found id:" + matchingWorkflow.getId());
		    
//			Date date = new Date();
//			SimpleDateFormat ft = new SimpleDateFormat ("'" + historyName + " -' yyyy.MM.dd  hh:mm:ss a");
//			historyName = ft.format(date);
   
		    final WorkflowInputs inputs = new WorkflowInputs();
//		    inputs.setDestination(new WorkflowInputs.NewHistory(historyName) );
		    inputs.setDestination(destination);
		    inputs.setWorkflowId(matchingWorkflow.getId());
		    // setStepParameter(String stepId, String parameterName, Object parameterValue) 
		    // Adds an element to the Java Map WorkflowInputs.parameters, 
		    // which when it arrives at Galaxy's Python backend will be a Python value in the following form:
		    //------------------
			//		    'parameters': 
			//			{
			//				'<stepId>': 
			//					{'<parameterName>': 
			//						<parameterValue> 
			//					}, 
			//				'step495': {'globus_send_data': [{'value': '~/matched/s00402/3793588_0035_wqrs.txt', 'param': 'to_path'}]}, 
			//				'step487': {'globus_get_data': [{'value': '~/matched/s00402/3793588_0035.hea', 'param': 'from_path'}]}, 
			//				'step486': {'globus_get_data': [{'value': '~/matched/s00402/3793588_0035.dat', 'param': 'from_path'}]}
			//			}, 
		    // -----------------
		    // parameterValue is expected to be a single element Hashmap Array, with the element being a Hashmap with the keys "value" and "param".
		    // which yields the following Python value at Galaxy:
		    // [{'value': '<valueString>', 'param': '<paramString>'}]
		    for(galaxyParameter gp : paramArray){
		    	inputs.setStepParameter(gp.getStepId(), gp.getStepParameterName(), buildParameterArray(gp.getValue(),gp.getParamName()));
		    }
		    ClientResponse output2=null;
		    try{
//			    final ClientResponse output1 = workflowsClient.runWorkflowResponse(inputs);
			    output2 = workflowsClient.runWorkflowResponse(inputs);
			    System.out.println("------ clientReponse:" + output2.getClientResponseStatus() + " header: " + paramArray[0].getValue());
		    }catch(GalaxyResponseException GRex){
			    System.err.println("ERROR, clientReponse:" + output2.getClientResponseStatus() + " header: " + paramArray[0].getValue());
			    System.err.println(GRex.getMessage());
		    }
		   
//		    final WorkflowOutputs output = workflowsClient.runWorkflow(inputs);
//		    System.out.println("Running workflow in history " + output.getHistoryId());
//		    for(String outputId : output.getOutputIds()) {
//		      System.out.println("  Workflow writing to output id " + outputId);
//		    }
		  }
}

class galaxyParameter{
	String value;
	String paramName;
	String stepId;
	String stepParameterName;
	
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getParamName() {
		return paramName;
	}
	public void setParamName(String paramName) {
		this.paramName = paramName;
	}
	public String getStepId() {
		return stepId;
	}
	public void setStepId(String stepId) {
		this.stepId = stepId;
	}
	public void setStepIdNumber(int number){
		this.stepId = "step"+number;
	}
	public String getStepParameterName() {
		return stepParameterName;
	}
	public void setStepParameterName(String stepParameterName) {
		this.stepParameterName = stepParameterName;
	}

}
