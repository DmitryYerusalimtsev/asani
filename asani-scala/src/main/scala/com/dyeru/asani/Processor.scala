package com.dyeru.asani

trait Processor[In, Out] {
  def command: String

  def process(in: List[In]): List[Out]
}
