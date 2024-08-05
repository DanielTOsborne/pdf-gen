from java.lang      import Exception
from java.awt       import BorderLayout
from java.awt       import Dimension
from java.awt       import FlowLayout
from java.awt       import GridBagLayout
from java.awt       import GridBagConstraints
from java.awt       import Frame
from java.awt.event import ActionListener
from javax.swing    import AbstractAction
from javax.swing    import JButton
from javax.swing    import JComboBox
from javax.swing    import JComponent
from javax.swing    import JDialog
from javax.swing    import JLabel
from javax.swing    import JPanel
from javax.swing    import JPasswordField
from javax.swing    import JTextField
from javax.swing    import KeyStroke
from javax.swing    import UIManager

import jarray, os, time, DBAPI

'''
Version Information
'''
VERSION_NUMBER = "19.5"
VERSION_DATE   = "05Sep019"

offices = {
    #	EROC :  OFC
    "B0" : "MVD",
    "B1" : "MVM",
    "B2" : "MVN",
    "B3" : "MVS",
    "B4" : "MVK",
    "B5" : "MVR",
    "B6" : "MVP",
    "E0" : "NAD",
    "E1" : "NAB",
    "E3" : "NAN",
    "E4" : "NAO",
    "E5" : "NAP",
    "E6" : "NAE",
    "G0" : "NWDP",
    "G2" : "NWP",
    "G3" : "NWS",
    "G4" : "NWW",
    "G5" : "NWK",
    "G6" : "NWO",
    "G7" : "NWDM",
    "H0" : "LRD",
    "H1" : "LRH",
    "H2" : "LRL",
    "H3" : "LRN",
    "H4" : "LRP",
    "H5" : "LRB",
    "H6" : "LRC",
    "H7" : "LRE",
    "H8" : "LRDG",
    "J0" : "POD",
    "J3" : "POH",
    "J4" : "POA",
    "K0" : "SAD",
    "K2" : "SAC",
    "K3" : "SAJ",
    "K5" : "SAM",
    "K6" : "SAS",
    "K7" : "SAW",
    "L0" : "SPD",
    "L1" : "SPL",
    "L2" : "SPK",
    "L3" : "SPN",
    "L4" : "SPA",
    "M0" : "SWD",
    "M2" : "SWF",
    "M3" : "SWG",
    "M4" : "SWL",
    "M5" : "SWT",
    "Q0" : "HEC",
    "S0" : "CPC",
}

##############################################################################

class NationalLoginDialog(JDialog) :
    '''
    UI to log into national CWMS database - not CAC-enabled
    '''
    #------------------#
    # useful constants #
    #------------------#
    PRODUCTION_LABEL = "Production"
    PRODUCTION_URL   = "140.194.20.214:1521:S0CWMSP2"
    PUBLIC_LABEL     = "Public (DMZ)"
    PUBLIC_URL       = "140.194.45.154:1521:S0CWMSZ2"
    #----------------#
    # helper classes #
    #----------------#
    class CancelButtonListener(ActionListener) :
        def __init__(self, dlg) : self._dlg = dlg
        def actionPerformed(self, evt) : self._dlg.setVisible(False)

    class EscapeKeyListener(AbstractAction) :
        def __init__(self, dlg) : self._dlg = dlg
        def actionPerformed(self, evt) : self._dlg.setVisible(False)

    class LoginButtonListener(ActionListener) :
        def __init__(self, dlg) : self._dlg = dlg
        def actionPerformed(self, evt) : self._dlg._canceled = False; self._dlg.setVisible(False)
    #-------------#
    # constructor #
    #-------------#
    def __init__(self, database=None, username=None, password=None, office=None) :
        '''
        constructor
        '''
        # if Frame.getFrames()[0] == None:
        JDialog.__init__(self, None, "Login to National Database", True)
        for frame in Frame.getFrames():
            if frame.getIconImage() != None:
                self.setIconImage(frame.getIconImage())

        # else :
        #     JDialog.__init__(Frame.getFrames()[0], None, "Login to National Database", True)

        #--------------------#
        # instance variables #
        #--------------------#
        self._canceled = True
        self._databaseSelector = JComboBox([NationalLoginDialog.PRODUCTION_LABEL, NationalLoginDialog.PUBLIC_LABEL])
        self._usernameField    = JTextField(32)
        self._passwordField    = JPasswordField(32)
        self._officeSelector   = JComboBox(sorted(offices.values()))
        #-----------------#
        # visual elements #
        #-----------------#
        rootPane = self.getRootPane()
        contentPane = JPanel(BorderLayout())
        inputPane = JPanel(GridBagLayout())
        c = GridBagConstraints()
        c.gridx, c.gridy = 0, 0
        c.ipadx = c.ipady = 5
        c.insets.top, c.insets.left, c.insets.bottom, c.insets.right = 2, 5, 2, 0
        c.weightx = c.weighty = 0.5
        inputPane.add(JLabel("Database:"), c)
        c.gridy = GridBagConstraints.RELATIVE
        inputPane.add(JLabel("Username:"), c)
        inputPane.add(JLabel("Password:"), c)
        inputPane.add(JLabel("Office:"), c)
        c.gridx, c.gridy = 1, 0
        c.insets.top, c.insets.left, c.insets.bottom, c.insets.right = 2, 0, 2, 5
        inputPane.add(self._databaseSelector, c)
        c.gridy = GridBagConstraints.RELATIVE
        inputPane.add(self._usernameField, c)
        inputPane.add(self._passwordField, c)
        inputPane.add(self._officeSelector, c)
        loginButton = JButton("Login")
        loginButton.addActionListener(NationalLoginDialog.LoginButtonListener(self))
        cancelButton = JButton("Cancel")
        cancelButton.addActionListener(NationalLoginDialog.CancelButtonListener(self))
        buttonPane = JPanel(FlowLayout())
        buttonPane.add(loginButton)
        buttonPane.add(cancelButton)
        contentPane.add(inputPane, BorderLayout.CENTER)
        contentPane.add(buttonPane, BorderLayout.SOUTH)
        self.setContentPane(contentPane)
        #----------------#
        # initial values #
        #----------------#
        if database :
            for i in range(self._databaseSelector.getItemCount()) :
                if self._databaseSelector.getItemAt(i).upper().startswith(database.upper()) :
                    self._databaseSelector.setSelectedIndex(i)
                    break
        if not username :
            username = os.getenv("USERNAME")
        if username :
            self._usernameField.setText(username)
        if password :
            self._passwordField.setText(password)
        if office :
            try    : self._officeSelector.setSelectedItem(office.upper())
            except : pass
        else :
            eroc = username[:2].upper()
            if eroc in offices : self._officeSelector.setSelectedItem(offices[eroc])
        #------------------------------------------#
        # default behaviors for Enter and Esc keys #
        #------------------------------------------#
        rootPane.setDefaultButton(loginButton)
        rootPane.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke("ESCAPE"), "ESCAPE")
        rootPane.getActionMap().put("ESCAPE", NationalLoginDialog.EscapeKeyListener(self))
        #-----------------------------#
        # finalize visual arrangement #
        #-----------------------------#
        self.pack()
        x = self._usernameField.getLocation().x
        self._databaseSelector.setLocation(x, self._databaseSelector.getLocation().y)
        self._officeSelector.setLocation(x, self._officeSelector.getLocation().y)
        size = Dimension(self._usernameField.getSize().width, self._databaseSelector.getSize().height)
        self._databaseSelector.setPreferredSize(size)
        size.height = self._officeSelector.getSize().height
        self._officeSelector.setPreferredSize(size)
        self.pack()
        self._passwordField.requestFocus()
        self.setLocationRelativeTo(None)
    #------------------#
    # instance methods #
    #------------------#
    def wasCanceled(self) :
        '''
        return whether dialog was canceled
        '''
        return self._canceled

    def getDatabase(self) :
        '''
        return the database URL
        '''
        if self._databaseSelector.getSelectedItem() == NationalLoginDialog.PRODUCTION_LABEL :
            return NationalLoginDialog.PRODUCTION_URL
        else :
            return NationalLoginDialog.PUBLIC_URL

    def getUsername(self) :
        '''
        return the user name
        '''
        return self._usernameField.getText()

    def getPassword(self) :
        '''
        return the password
        '''
        return self._passwordField.getText()

    def getOffice(self) :
        '''
        return the office
        '''
        return self._officeSelector.getSelectedItem()

    def getInfo(self) :
        '''
        return the URL, user name, and password
        '''
        return self.getDatabase(), self.getUsername(), self.getPassword(), self.getOffice()

##############################################################################


def getNationalDbLoginInfo(*args, **kwargs) :
    '''
    get national database URL, username, password, and office based on input
    '''
    #-----------------------#
    # process the arguments #
    #-----------------------#
    database = username = password = office = None
    argcount = len(args) + len(kwargs)
    if argcount > 4 :
        raise Exception("Expected 0-4 arguments, got %d" % argcount)
    for i in range(len(args)) :
        if   i == 0 : database = args[i]
        elif i == 1 : username = args[i]
        elif i == 2 : password = args[i]
        elif i == 3 : office   = args[i]
    for key in list(kwargs.keys()) :
        if key == "database" :
            if database : raise Exception("Parameter 'database' specified by position and keyword")
            database = kwargs[key]
        elif key == "username" :
            if username : raise Exception("Parameter 'username' specified by position and keyword")
            username = kwargs[key]
        elif key == "password" :
            if password : raise Exception("Parameter 'password' specified by position and keyword")
            password = kwargs[key]
        elif key == "office" :
            if office : raise Exception("Parameter 'office' specified by position and keyword")
            office = kwargs[key]
        else :
            raise Exception("Unexpected keyword argument: %s" % key)
    if all([database, username, password, office]) :
        if NationalLoginDialog.PRODUCTION_LABEL.upper().startswith(database.upper()) or NationalLoginDialog.PRODUCTION_URL.upper().endswith(database.upper()) :
            database = NationalLoginDialog.PRODUCTION_URL
        elif NationalLoginDialog.PUBLIC_LABEL.upper().startswith(database.upper()) or NationalLoginDialog.PUBLIC_URL.upper().endswith(database.upper()) :
            database = NationalLoginDialog.PUBLIC_URL
        else :
            raise ValueError("Invalid national database: %s" % database)
        return database, username, password, office
    try    : UIManager.setLookAndFeel("com.sun.java.swing.plaf.windows.WindowsLookAndFeel")
    except : pass
    dlg = NationalLoginDialog(database, username, password, office)
    dlg.setVisible(True)
    while dlg.isVisible() : time.sleep(.25)
    if dlg.wasCanceled() :
        loginInfo = None
    else :
        loginInfo = dlg.getInfo()
    del dlg
    return loginInfo

def openNational(*args, **kwargs) :
    '''
    Return a new DbAccess object to a national database
    '''
    info = getNationalDbLoginInfo(*args, **kwargs)
    if info : return DBAPI.DbAccess(*info)
