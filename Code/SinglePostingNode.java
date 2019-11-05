
public class SinglePostingNode {

	int docId;
	SinglePostingNode next;
	Boolean skipcheck;
	SinglePostingNode skip;
	
	public SinglePostingNode(int docId) {
		// TODO Auto-generated constructor stub
		this.docId=docId;
		next=null;
		skip=null;
	}
}
