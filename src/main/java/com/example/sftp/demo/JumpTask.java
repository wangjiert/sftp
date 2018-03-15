package com.example.sftp.demo;

import java.util.concurrent.RecursiveAction;

public class JumpTask extends RecursiveAction {
	private String name;
	public JumpTask(String name) {
		this.name = name;
	}

	@Override
	protected void compute() {
		
	}

}
