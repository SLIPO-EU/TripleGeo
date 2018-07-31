package eu.slipo.athenarc.triplegeo.osm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Class containing information about the OSM relations.
 * 
 * @author Nikos Karagiannakis
 * Modified: 7/9/2017 by Kostas Patroumpas
 * Last revised: 27/6/2018 by Kostas Patroumpas
 */
public class OSMRelation implements Serializable{
    
    private static final long serialVersionUID = 1L;    
    private String id;
    private Set<Integer> classIDs;
    private final Map<String, ImmutablePair<String, String>> memberReferences = new HashMap<>();
    private final Map<String, String> tags = new HashMap<>();
    
    public String getID(){
        return id;
    }
    
    public Map<String, ImmutablePair<String, String>> getMemberReferences(){
        return memberReferences;
    }
    
    public Set<Integer> getClassIDs(){
        return this.classIDs;
    }
    
    public Map<String, String> getTagKeyValue(){
        return tags;
    }
    
    public void setID(String id){
        this.id = id;
    }
    
    public void setClassIDs(Set<Integer> classIDs){
        this.classIDs = classIDs;
    }    
    
    public void addMemberReference(String memberReference, String type, String role){
        this.memberReferences.put(memberReference, new ImmutablePair<>(type, role));        //Type and role of this OSM element are stored as the value of this member reference
    }    
    
    public void setTagKeyValue(String tagKey, String tagValue){
        this.tags.put(tagKey, tagValue);
    }
}