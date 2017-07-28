package eu.slipo.athenarc.triplegeo.osm;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
//import de.bwaldvogel.liblinear.FeatureNode;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/**
 * Class containing information about the OSM ways.
 * 
 * @author Nikos Karagiannakis
 */

public class OSMWay implements Serializable{
    
    private static final long serialVersionUID = 1L;   
    private String id;   
    private int classID;
    private Set<Integer> classIDs;   
    private final List<String> nodeReferences = new ArrayList<String>(); //node references  //made final
    private final List<Geometry> nodeGeometries = new ArrayList<Geometry>(); //nodeGeometries   //made final
    private Coordinate[] coordinateList;    
    private final Map<String, String> tags = new HashMap<>();      
    private Geometry geometry;
    private TreeMap<Integer,Double> indexVector = new TreeMap<>();   
 //   private ArrayList<FeatureNode> featureNodeList = new ArrayList<>();
    
    //way attributes getters 
    public String getID(){
        return id;
    } 
    
    public List<Geometry> getNodeGeometries(){
        return nodeGeometries;
    }
    
    public Coordinate[] getCoordinateList(){       
        coordinateList =  (Coordinate[]) nodeGeometries.toArray();
        return coordinateList;
    }
    
    public Geometry getGeometry(){
        return geometry;
    }   
    
    public List<String> getNodeReferences(){
        return nodeReferences;
    }
    
    public int getNumberOfNodes(){
        return nodeReferences.size();
    }
    
    public Map<String, String> getTagKeyValue(){
        return tags;
    }
    
    public int getClassID(){
        return classID;
    }
    
    public Set<Integer> getClassIDs(){
        return classIDs;
    }
      
    public TreeMap<Integer, Double> getIndexVector(){
        return indexVector;
    }
    
    public void setIndexVector(TreeMap<Integer, Double> indexVector){
        this.indexVector = indexVector;
    }
    
    //way attributes setters
    public void setID(String id){
        this.id = id;
    }
    
    public void setTagKeyValue(String tagKey, String tagValue){
        this.tags.put(tagKey, tagValue);
    }
    
    public void addNodeReference(String nodeReference){
        nodeReferences.add(nodeReference);
    }
    
    public void addNodeGeometry(Geometry geometry){
        nodeGeometries.add(geometry);
    }
    
    public void setGeometry(Geometry geometry){       
        this.geometry = geometry;
    }  
    
    public void setClassID(int classID){
        this.classID = classID;
    }

    public void setClassIDs(Set<Integer> classIDs){
        this.classIDs = classIDs;
    }  
 /*   
    public void setFeature(FeatureNode featureNode){
        this.featureNodeList.add(featureNode);
    }
    
    public List<FeatureNode> getFeatureNodeList(){
        return featureNodeList;
    }
    */
}