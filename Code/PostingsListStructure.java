
public class PostingsListStructure {

	SinglePostingNode head;
	SinglePostingNode tail;
	int count;
	int comparecount;
	
	public PostingsListStructure() {
		head=null;
		tail=null;
		count=0;
		comparecount=0;
		// TODO Auto-generated constructor stub
	}
	
	void insert(int docId)
	{
		SinglePostingNode node= new SinglePostingNode(docId);
		if(head== null) {
			head = node;
			tail = node;
		}
		
		else {
			tail.next = node;
			tail = node;
		}
		count++;
	}
	
}
