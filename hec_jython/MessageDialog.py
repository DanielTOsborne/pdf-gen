'''
Module MessageDialog

This module mimics the functionality of the hec.script.MessageBox class
with the exception that these dialogs are modeless, allowing interaction
with other elements (tables, plots, etc...) while the script is paused
waiting on input to the message box

Mike Perryman
USACE HEC
25 Mar 2004

'''

from java.awt import *
from java.awt.event import *
from javax.swing import *

from hec.script.Constants import *

import time

def showError(message, title) :
	'''
	Show dialog with error icon and OK button
	'''
	icon = UIManager.get("OptionPane.errorIcon")
	buttonTexts = ["OK"]
	dlg = MessageDialog(message, title, icon, buttonTexts)
	dlg.setVisible(TRUE)
	buttonPressed = dlg.getButtonPressed()
	dlg.close()
	return buttonPressed

def showWarning(message, title) :
	'''
	Show dialog with warning icon and OK button
	'''
	icon = UIManager.get("OptionPane.warningIcon")
	buttonTexts = ["OK"]
	dlg = MessageDialog(message, title, icon, buttonTexts)
	dlg.setVisible(TRUE)
	buttonPressed = dlg.getButtonPressed()
	dlg.close()
	return buttonPressed

def showInformation(message, title) :
	'''
	Show dialog with error icon and OK button
	'''
	icon = UIManager.get("OptionPane.informationIcon")
	buttonTexts = ["OK"]
	dlg = MessageDialog(message, title, icon, buttonTexts)
	dlg.setVisible(TRUE)
	buttonPressed = dlg.getButtonPressed()
	dlg.close()
	return buttonPressed

def showPlain(message, title) :
	'''
	Show dialog with no icon and OK button
	'''
	buttonTexts = ["OK"]
	dlg = MessageDialog(message, title, None, buttonTexts)
	dlg.setVisible(TRUE)
	buttonPressed = dlg.getButtonPressed()
	dlg.close()
	return buttonPressed

def showYesNo(message, title) :
	'''
	Show dialog with question icon and Yes and No buttons
	'''
	icon = UIManager.get("OptionPane.questionIcon")
	buttonTexts = ["Yes", "No"]
	dlg = MessageDialog(message, title, icon, buttonTexts)
	dlg.setVisible(TRUE)
	buttonPressed = dlg.getButtonPressed()
	dlg.close()
	return buttonPressed

def showYesNoCancel(message, title) :
	'''
	Show dialog with question icon and Yes, No and Cancel buttons
	'''
	icon = UIManager.get("OptionPane.questionIcon")
	buttonTexts = ["Yes", "No", "Cancel"]
	dlg = MessageDialog(message, title, icon, buttonTexts)
	dlg.setVisible(TRUE)
	buttonPressed = dlg.getButtonPressed()
	dlg.close()
	return buttonPressed

def showOkCancel(message, title) :
	'''
	Show dialog with question icon and OK and Cancel buttons
	'''
	icon = UIManager.get("OptionPane.questionIcon")
	buttonTexts = ["OK", "Cancel"]
	dlg = MessageDialog(message, title, icon, buttonTexts)
	print(dlg)
	dlg.setVisible(TRUE)
	buttonPressed = dlg.getButtonPressed()
	dlg.close()
	return buttonPressed

class MessageDialog(ActionListener) :
	'''
	A modeless message box class to mimic the (modal) MessageBox class
	'''
	def  __init__(self, message, title, icon, buttonTexts) :
		'''
		constructor
		'''
		#----------------------#
		# initialize variables #
		#----------------------#
		self._dlg_ = None
		self._buttons_ = []
		self._buttonPressed_ = None
		font = UIManager.get("OptionPane.font")
		buttonCount = len(buttonTexts)

		#----------------------------------------------#
		# create the dialog off-screen and set visible #
		# to determine its "empty" height (getSize()   #
		# returns [width=0,height=0] before the dialog #
		# is visible)                                  #
		#----------------------------------------------#
		self._dlg_ = JDialog()
		self._dlg_.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE)
		self._dlg_.setLocation(-1000, -1000)
		self._dlg_.setVisible(TRUE)
		baseHeight = self._dlg_.getSize().height

		#---------------------------------------------------#
		# create the buttons, add this object as a listener #
		# to each one, and size them uniformly              #
		#---------------------------------------------------#
		buttonWidth = 0
		buttonHeight = 0
		for buttonText in buttonTexts :
			button = JButton(buttonText)
			button.setFont(font)
			button.addActionListener(self)
			size = button.getPreferredSize()
			if size.width > buttonWidth : buttonWidth = size.width
			if size.height > buttonHeight : buttonHeight = size.height
			self._buttons_.append(button)

		for button in self._buttons_ :
			button.setPreferredSize(Dimension(buttonWidth, buttonHeight))
			button.setSize(buttonWidth, buttonHeight)

		#----------------------------------------------------#
		# use 1/2 button height as a universal spacing value #
		#----------------------------------------------------#
		spacing = buttonHeight / 2

		#-----------------------------------#
		# build a panel to hold the buttons #
		#-----------------------------------#
		buttonPanel = Box.createHorizontalBox()
		buttonPanel.add(Box.createGlue())
		for i in range(buttonCount) :
			if i > 0 : buttonPanel.add(Box.createHorizontalStrut(spacing))
			buttonPanel.add(self._buttons_[i])
		buttonPanel.add(Box.createGlue())
		buttonPanel.setSize(
			buttonWidth * buttonCount + spacing * (buttonCount + 1),
			buttonHeight + spacing)

		#-----------------#
		# set up the icon #
		#-----------------#
		if icon :
			iconLabel = JLabel(icon)
			iconSize = iconLabel.getPreferredSize()
			iconLabel.setSize(iconSize)

		#--------------------#
		# set up the message #
		#--------------------#
		if message :
			messageLabel = JLabel(message)
			messageLabel.setFont(font)
			messageSize = messageLabel.getPreferredSize()
			messageLabel.setSize(messageSize)

		#--------------------------------------------#
		# build a panel to hold the icon and message #
		#--------------------------------------------#
		if icon or message :
			messagePanel = Box.createHorizontalBox()
			messagePanel.add(Box.createGlue())
			if icon :
				messagePanel.add(iconLabel)
				if message :
					messagePanel.add(Box.createHorizontalStrut(spacing))
					messagePanel.add(messageLabel)
					width = iconSize.width + messageSize.width + spacing * 3
					height = max(iconSize.height, messageSize.height) + spacing * 3
				else :
					width = iconSize.width + spacing * 2
					height = iconSize.height + spacing * 3
			else :
				messagePanel.add(messageLabel)
				width = messageSize.width + spacing * 2
				height = messageSize.height + spacing * 3

			messagePanel.add(Box.createGlue())
			messagePanel.setSize(Dimension(width, height))

		#----------------------------------#
		# build a panel to hold everything #
		#----------------------------------#
		contentsPanel = Box.createVerticalBox()
		contentsPanel.add(Box.createGlue())
		if icon or message :
			contentsPanel.add(messagePanel)
			contentsPanel.add(Box.createVerticalStrut(spacing))
		contentsPanel.add(buttonPanel)
		contentsPanel.add(Box.createGlue())
		if icon or message :
			contentsPanel.setSize(
				max(messagePanel.getSize().width, buttonPanel.getSize().width),
				messagePanel.getSize().height + buttonPanel.getSize().height)
		else :
			contentsPanel.setSize(buttonPanel.getSize())

		#---------------------------------------------------------------------------#
		# set the dialog's content pane to be our panel centered in a border layout #
		#---------------------------------------------------------------------------#
		contentPane = JPanel(BorderLayout())
		contentPane.add(contentsPanel, BorderLayout.CENTER)
		contentPane.setSize(contentsPanel.getSize())
		self._dlg_.setContentPane(contentPane)
		self._dlg_.setSize(contentPane.getWidth(), baseHeight + contentPane.getHeight())

		#---------------#
		# add the title #
		#---------------#
		if title : self._dlg_.setTitle(title)

		#-----------------------------------------#
		# move the dialog to the default position #
		#-----------------------------------------#
		screenSize = Toolkit.getDefaultToolkit().getScreenSize()
		dlgSize = self._dlg_.getSize()
		self._dlg_.setLocation(
			(screenSize.width - dlgSize.width) / 2,
			(screenSize.height - dlgSize.height) / 2)


	#------------------#
	# instance methods #
	#------------------#
	def actionPerformed(self, event) :
		'''
		Callback method for dialog action events
		Required by Interface ActionListener
		'''
		if event.getSource() in self._buttons_ :
			self._dlg_.dispose()
			self._buttonPressed_ = event.getActionCommand()

	def getButtonPressed(self) :
		'''
		Waits for an button press and returns
		'''
		while self._buttonPressed_ == None : time.sleep(.05)
		buttonPressed = self._buttonPressed_
		self._buttonPressed_ = None
		return buttonPressed

	def close(self) :
		'''
		Clear out the dialog
		'''
		self._dlg_.dispose()

	def setVisible(self, state) :
		'''
		set the visibility
		'''
		self._dlg_.setVisible(state)
