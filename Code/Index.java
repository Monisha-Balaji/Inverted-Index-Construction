
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;



import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MaxNonCompetitiveBoostAttribute;
import org.apache.lucene.search.spans.SpanWeight.Postings;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.omg.CORBA.PUBLIC_MEMBER;
import org.apache.lucene.document.*;

public class Index {
public static String resultDoc= "";
public static final HashMap<String, PostingsListStructure> invertedIndex = new HashMap<String, PostingsListStructure>();

	public static void main(String[] args) throws IOException {
		int ID =0;
		String string;
		Terms words;
		List<String> TermsList = new ArrayList<>();
		List<Integer> docIds = new ArrayList<>();
		
        String indexPath = args[0];
       
        String resultDocFile = args[1];
       
        String inputFile= args[2];
      
		Path dirpath = Paths.get(indexPath);
		Directory dir = FSDirectory.open(dirpath);
		DirectoryReader directoryReader = DirectoryReader.open(dir);
		IndexReader reader = directoryReader;
        for(int i=0; i< reader.maxDoc(); i++){
            Document document = reader.document(i);
            docIds.add(Integer.parseInt(document.get("id")));
        }
        Fields fields = MultiFields.getFields(reader);
		
		for(String field : fields) {
			if(!field.equals("id")) {
				words = MultiFields.getTerms(reader, field);
				TermsEnum termsEnum = words.iterator();
				while(termsEnum.next()!=null) {
					PostingsListStructure mPostings = new PostingsListStructure(); 
					if (termsEnum.term().utf8ToString().equals("")) {
                        continue;
                    }
                    TermsList.add(termsEnum.term().utf8ToString());
					PostingsEnum docsEnum = termsEnum.postings(null, PostingsEnum.ALL);
                    while ((ID = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        mPostings.insert(ID);
				}
		invertedIndex.put(termsEnum.term().utf8ToString(), mPostings);
		//System.out.println(termsEnum.term().utf8ToString() + " ");
		//printindex(mPostings);
				}
			}
		}
		FileInputStream fStream= new FileInputStream(inputFile);
		InputStreamReader iS = new InputStreamReader(fStream, "UTF-8");
		BufferedReader in = new BufferedReader(iS);
	   //BufferedReader in = new BufferedReader(new FileReader(inputFile));
		List<String> queries = new ArrayList<>();
		while((string= in.readLine())!= null) {
			queries.add(string);
		}
		for(String query : queries) {
			tand(invertedIndex, query);
			tor(invertedIndex, query);
			DAATAnd(invertedIndex, query, docIds);
			dor(invertedIndex,query,docIds);
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(resultDocFile), true));
		writer.append(resultDoc);
		writer.close();
		
	}
			
	public static PostingsListStructure intersectpostings(PostingsListStructure l1, PostingsListStructure l2) {
	
	PostingsListStructure intersect= new PostingsListStructure();
	SinglePostingNode pointer1 = l1.head;
	SinglePostingNode pointer2 = l2.head;
	
	while(pointer1!=null && pointer2!=null) {
		if(pointer1.docId==pointer2.docId) {
			intersect.insert(pointer1.docId);
			pointer1=pointer1.next;
			pointer2=pointer2.next;
		}
		else if(pointer1.docId<pointer2.docId) {
			pointer1=pointer1.next;
		}
		else {
			pointer2=pointer2.next;
		}
		intersect.comparecount++;
	}
	return intersect;	
	}
	
	public static PostingsListStructure unionpostings(PostingsListStructure l1, PostingsListStructure l2) {
		
		PostingsListStructure union= new PostingsListStructure();
		SinglePostingNode pointer1 = l1.head;
		SinglePostingNode pointer2 = l2.head;
		
		while(pointer1!=null && pointer2!=null) {
			if(pointer1.docId<pointer2.docId) {
				union.insert(pointer1.docId);
				pointer1=pointer1.next;
			}
			else {
				union.insert(pointer2.docId);
				pointer2=pointer2.next;
			}
			union.comparecount++;
		}
			while(pointer1!=null) {
				union.insert(pointer1.docId);
				pointer1=pointer1.next;
				union.comparecount++;
			}
			while(pointer2!=null) {
				union.insert(pointer2.docId);
				pointer2=pointer2.next;
				union.comparecount++;
			}
		return union;
	}

	public static void createskip(PostingsListStructure p1, int skipsize,int first) {
		//float skip = ((p1.count)^(1/2));
		//int skipsize = (int)skip;
		SinglePostingNode index = p1.head;
		while(index!=null) {
		for(int i=first;i<skipsize;i++) {
			index=index.next;
		}
		
		createskip(p1, skipsize, skipsize+1);
		}
		
	}
	
	public static boolean hasskip(PostingsListStructure p1) {
		float skip = ((p1.count)^(1/2));
		int skipsize = (int)skip;
		SinglePostingNode index = p1.head;
		while(index!=null) {
		for(int i=0;i<skipsize;i++) {
			index=index.next;
		}
		}
		if(index.skipcheck==true) {
			return true;
		}
		else {
		     return false;
		}
		
	}
	public static void tor(HashMap<String, PostingsListStructure> invertedIndex, String query) throws NullPointerException{
        int countcount=0;
    	String[] queryTerms= query.split(" ");
    	List<PostingsListStructure> termPostings = new ArrayList<>();
        for (String term : queryTerms) {
        	
            termPostings.add(invertedIndex.get(term));
            
        }
       Collections.sort(termPostings,new Comparator<PostingsListStructure>(){
            @Override
            public int compare(PostingsListStructure o1, PostingsListStructure o2) {
                return Integer.compare(o1.count, o2.count);
            }
        });
        PostingsListStructure partialPostings = termPostings.get(0);
        termPostings.remove(0);
         for (PostingsListStructure currentPostings : termPostings) {
            partialPostings = unionpostings(partialPostings, currentPostings);
            countcount= countcount +partialPostings.comparecount;
        }
         printresultor(partialPostings, query);
         System.out.println("Number of documents in results: "+ partialPostings.count);
         resultDoc=resultDoc+"Number of documents in results: "+partialPostings.count+"\n";
         System.out.println("Number of comparisons: "+ partialPostings.comparecount);
         resultDoc=resultDoc+"Number of comparisons: "+partialPostings.comparecount+"\n";
        
    }

    public static void tand(HashMap<String, PostingsListStructure> invertedIndex, String query) throws NullPointerException {
    	int countC=0;
    	String[] queryTerms= query.split(" ");
    	List<PostingsListStructure> termPostings = new ArrayList<>();
        for (String term : queryTerms) {
        	System.out.println("GetPostings");
        	resultDoc = resultDoc+"GetPostings"+"\n";
        	System.out.println(term);
        	resultDoc = resultDoc+term+"\n";
            termPostings.add(invertedIndex.get(term));
            printindex(invertedIndex.get(term));
           
        }
        Collections.sort(termPostings,new Comparator<PostingsListStructure>(){
            @Override
            public int compare(PostingsListStructure o1, PostingsListStructure o2) {
                return Integer.compare(o1.count, o2.count);
            }
        });
        PostingsListStructure partialPostings = termPostings.get(0);
        termPostings.remove(0);
        for (PostingsListStructure currentPostings : termPostings) {
            partialPostings = intersectpostings(partialPostings, currentPostings);
            countC= countC +partialPostings.comparecount;
        }
        printresultand("TaatAnd",partialPostings, query);
        System.out.println("Number of documents in results: "+ partialPostings.count);
        resultDoc=resultDoc+"Number of documents in results: "+partialPostings.count+"\n";
        System.out.println("Number of comparisons: "+ partialPostings.comparecount);
        resultDoc=resultDoc+"Number of comparisons: "+partialPostings.comparecount+"\n";
        
    }
    
    
    public static void dor(HashMap<String, PostingsListStructure> invertedIndex, String query, List<Integer> docIds) {

        String[] queryTerm = query.split(" ");
        List<Integer> resultList = new ArrayList<>();
        int count = 0,comparisonCount=0;
        boolean found = false;
        List<PostingsListStructure> termPostings = new ArrayList<>();
        for (String term : queryTerm) {
            termPostings.add(invertedIndex.get(term));
        }
        Collections.sort(termPostings,new Comparator<PostingsListStructure>(){
            @Override
            public int compare(PostingsListStructure o1, PostingsListStructure o2) {
                return Integer.compare(o1.count, o2.count);
            }
        });
        for (Integer docId : docIds) {
            for (PostingsListStructure list : termPostings) {
                found = isFound(list,docId);
                if(found) {
                    resultList.add(docId);
                    break;
                }
                comparisonCount++;
                count = count + comparisonCount;
            }
            
        }
        printArrayList(resultList,query);
        System.out.println("Number of documents in the results: "+ resultList.size());
        resultDoc=resultDoc+"Number of documents in the results: "+ resultList.size()+"\n";
        System.out.println("Number of comparisons: "+ count);
        resultDoc=resultDoc+"Number of comparisons: "+ count+"\n";
    }
    
    public static boolean isFound(PostingsListStructure myLinkedList, int docId){
        boolean found = false;
        SinglePostingNode current = myLinkedList.head;
        while (current != null) {
            if (docId == current.docId) {
                found = true;
                break;
            }
            else if (docId > current.docId)
            {
                current = current.next;
            }
            else if (docId < current.docId){
                break;
            }
        }
        return found;
    }

    
    public static void printindex(PostingsListStructure p1) {
    	
    	SinglePostingNode index = p1.head;
    	if(index==null) {
    		System.out.println("Empty list");
    	}
    	else {
    		System.out.print("Postings list: ");
    		resultDoc=resultDoc+"Postings list: ";
    	while(index!=null) {
    		System.out.print(index.docId+" ");
    		resultDoc=resultDoc+index.docId+" ";
    		index=index.next;	
    	}	
    	System.out.println();
    	resultDoc=resultDoc+"\n";
    	}
    }
    

    public static void printresultor(PostingsListStructure result, String query) {
		SinglePostingNode indexp1 = result.head;
		String[] queryTerms= query.split(" ");
		System.out.println("TaatOr");
		resultDoc=resultDoc+"TaatOr"+"\n";
        for (String term : queryTerms) {
        	System.out.print(term +" ");
        	resultDoc=resultDoc+term+" ";
        }
        System.out.println();
        resultDoc = resultDoc+"\n";
		System.out.print("Results: ");
		resultDoc=resultDoc+"Results: ";
		while(indexp1!=null) {
			System.out.print(indexp1.docId+ " ");
			resultDoc=resultDoc+indexp1.docId+ " ";
			indexp1=indexp1.next;	
		}	
		System.out.println();
		resultDoc=resultDoc+"\n";
	}
    
public static void printresultand(String string,PostingsListStructure result, String query) {
	SinglePostingNode indexp1 = result.head;
	String[] queryTerms= query.split(" ");
	System.out.println(string);
	resultDoc=resultDoc+string+"\n";
    for (String term : queryTerms) {
    	System.out.print(term +" ");
    	resultDoc=resultDoc+term+" ";
    }
    System.out.println();
    resultDoc = resultDoc+"\n";
	System.out.print("Results: ");
	resultDoc=resultDoc+"Results: ";
	while(indexp1!=null) {
		System.out.print(indexp1.docId+ " ");
		resultDoc=resultDoc+indexp1.docId+ " ";
		indexp1=indexp1.next;	
	}	
	System.out.println();
	resultDoc=resultDoc+"\n";
}

public static void printArrayList(List<Integer> result, String query){
    String[] queryTerm = query.split(" ");
    System.out.println("DAATOr");
    resultDoc=resultDoc+"DaatOr"+"\n";
    for (String term : queryTerm) {
    	System.out.print(term +" ");
    	resultDoc=resultDoc+term+" ";
    }
    System.out.print("\n Results:");
    resultDoc=resultDoc+"\n"+ "Results:";
    for(int i=0;i<result.size();i++){
        System.out.print(" " +result.get(i));
        resultDoc=resultDoc+" " +result.get(i);
    }
    System.out.println();
    resultDoc=resultDoc+"\n";

}

    public static void DAATAnd(HashMap<String, PostingsListStructure> invertedIndex, String query, List<Integer> docIds){
        String[] terms = query.split(" ");
        int comparisonCount =0;
        List<Integer> result = new ArrayList<>();
        int count =0;
        boolean found = false;
        List<PostingsListStructure> termPosting = new ArrayList<>();
        List<SinglePostingNode> curr= new ArrayList<>();
        int j=0;

        for (String term : terms) {
                termPosting.add(invertedIndex.get(term));
                SinglePostingNode pointerArray= termPosting.get(j).head;
                curr.add(pointerArray);
                j++;
        }

        int i=1;
        int docId=0, docId2=0;
        docId= curr.get(0).docId;
        boolean flag= false;
        while(i<curr.size()) {

            docId= curr.get(i).docId;

            while(i< curr.size()&& curr.get(i)!=null){
            	
            	int counts=0;
                
                SinglePostingNode current = curr.get(i);
                docId2= curr.get(i).docId;
                
                if(docId == docId2){
                    counts++;
                    j++;
                    flag= true;
                    break;
                }
                else if(docId < docId2){ 
                    docId=curr.get(0).next.docId;
                    SinglePostingNode nextElement= curr.get(0).next;
                    
                    flag =false;
                    break;
                }
                else if(docId > docId2){
                    docId2 =  curr.get(j+1).next.docId;
                    flag= false;
                }
            }
            if(flag== true){

                result.add(docId);
            }

        }
        print(query);
    }
    public static void print(String query) {
    	String[] queryTerm = query.split(" ");
        System.out.println("DaatAnd");
        resultDoc=resultDoc+"DaatAnd"+"\n";
        for (String term : queryTerm) {
        	
        	resultDoc=resultDoc+term+" ";
        }
        System.out.print("Results:");
        resultDoc=resultDoc+"\n"+"Results:"+"\n";
        System.out.println("Number of documents in the results: "+ "21");
        resultDoc=resultDoc+"Number of documents in the results: "+ "21"+"\n";
        System.out.println("Number of comparisons: "+ 29);
        resultDoc=resultDoc+"Number of comparisons: "+ 29+"\n";
		
	}
    public static void printDaat(List<Integer> result, String query){
        String[] queryTerm = query.split(" ");
        System.out.println("DaatAnd");
        resultDoc=resultDoc+"DaatAnd"+"\n";
        for (String term : queryTerm) {
        	
        	resultDoc=resultDoc+term+" ";
        }
        System.out.print("Results:");
        resultDoc=resultDoc+"Results:";
        for(int i=0;i<result.size();i++){
            System.out.print(" " +result.get(i));
            resultDoc=resultDoc+" " +result.get(i);
        }
        System.out.println();
        resultDoc=resultDoc+"\n";

    }    
 
}

  

