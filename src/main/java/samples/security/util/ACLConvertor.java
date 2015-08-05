package samples.security.util;

import java.io.OutputStream;
import java.util.Stack;

/**
 * Converts ACL tags from prefix to infix
 * @author gman
 *
 */
public class ACLConvertor {

	
	
	/**
	 * This algorithm is a non-tail recursive method. 
1.The reversed input string is completely pushed into a stack. 
	prefixToInfix(stack) 
2.IF stack is not empty 
a. Temp -->pop the stack 
b. IF temp is a operator 
		Write a opening parenthesis to output 
		prefixToInfix(stack) 
		Write temp to output 
		prefixToInfix(stack) 
		Write a closing parenthesis to output 
c. ELSE IF temp is a space -->prefixToInfix(stack) 
d. ELSE 
		Write temp to output 
		IF stack.top NOT EQUAL to space -->prefixToInfix(stack)
	 * @param args
	 */
	
	public String prefixToInfix(Stack<String> stack) {
		String ret = "";
		
		if(!stack.empty()) {
			String tmp = stack.pop();
		}
		
		return null;
	}
	
	
	public static void main(String[] args) {
		
		
	}
	
	
}
