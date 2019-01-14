package org.eclipse.epsilon.epl.parse;

// $ANTLR 3.1b1 EolParserRules.g 2019-01-12 15:18:23

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;

/*******************************************************************************
 * Copyright (c) 2008 The University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Dimitrios Kolovos - initial API and implementation
 * -----------------------------------------------------------------------------
 * ANTLR 3 License
 * [The "BSD licence"]
 * Copyright (c) 2005-2008 Terence Parr
 * All rights reserved.
 *  
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
public class Epl_EolParserRules extends org.eclipse.epsilon.common.parse.EpsilonParser {
    public static final int T__144=144;
    public static final int T__143=143;
    public static final int T__146=146;
    public static final int MODELDECLARATIONPARAMETER=73;
    public static final int T__145=145;
    public static final int BREAKALL=39;
    public static final int T__140=140;
    public static final int T__142=142;
    public static final int VAR=48;
    public static final int MODELDECLARATIONPARAMETERS=72;
    public static final int T__141=141;
    public static final int THROW=53;
    public static final int PARAMLIST=25;
    public static final int EXPRLIST=54;
    public static final int EXPRRANGE=55;
    public static final int BREAK=38;
    public static final int ELSE=32;
    public static final int T__137=137;
    public static final int T__136=136;
    public static final int FORMAL=24;
    public static final int IF=31;
    public static final int MultiplicativeExpression=57;
    public static final int TYPE=65;
    public static final int T__139=139;
    public static final int T__138=138;
    public static final int T__133=133;
    public static final int T__132=132;
    public static final int T__135=135;
    public static final int T__134=134;
    public static final int T__131=131;
    public static final int NewExpression=47;
    public static final int T__130=130;
    public static final int CASE=35;
    public static final int Letter=16;
    public static final int LINE_COMMENT=22;
    public static final int T__129=129;
    public static final int T__126=126;
    public static final int JavaIDDigit=18;
    public static final int T__125=125;
    public static final int LAMBDAEXPR=64;
    public static final int MAP=75;
    public static final int T__128=128;
    public static final int T__127=127;
    public static final int T__166=166;
    public static final int T__165=165;
    public static final int T__168=168;
    public static final int DOMAIN=84;
    public static final int T__167=167;
    public static final int T__162=162;
    public static final int T__161=161;
    public static final int T__164=164;
    public static final int MODELDECLARATION=68;
    public static final int T__163=163;
    public static final int EXPRESSIONINBRACKETS=59;
    public static final int T__160=160;
    public static final int TRANSACTION=41;
    public static final int FLOAT_TYPE_SUFFIX=7;
    public static final int ITEMSELECTOR=74;
    public static final int COMMENT=21;
    public static final int ModelElementType=45;
    public static final int ROLE=85;
    public static final int IMPORT=67;
    public static final int DELETE=52;
    public static final int ARROW=11;
    public static final int T__159=159;
    public static final int T__158=158;
    public static final int T__155=155;
    public static final int SPECIAL_ASSIGNMENT=27;
    public static final int T__154=154;
    public static final int T__157=157;
    public static final int T__156=156;
    public static final int T__151=151;
    public static final int T__150=150;
    public static final int T__153=153;
    public static final int T__152=152;
    public static final int Annotation=23;
    public static final int CONTINUE=40;
    public static final int ENUMERATION_VALUE=66;
    public static final int OPERATOR=58;
    public static final int EXPONENT=6;
    public static final int STRING=14;
    public static final int T__148=148;
    public static final int T__147=147;
    public static final int T__149=149;
    public static final int T__100=100;
    public static final int NAMESPACE=69;
    public static final int COLLECTION=42;
    public static final int NEW=49;
    public static final int EXTENDS=80;
    public static final int T__102=102;
    public static final int PRE=78;
    public static final int T__94=94;
    public static final int T__101=101;
    public static final int POST=79;
    public static final int ALIAS=70;
    public static final int DRIVER=71;
    public static final int DO=89;
    public static final int OPTIONAL=91;
    public static final int KEYVAL=76;
    public static final int POINT_POINT=10;
    public static final int GUARD=81;
    public static final int T__99=99;
    public static final int T__95=95;
    public static final int HELPERMETHOD=28;
    public static final int T__96=96;
    public static final int T__97=97;
    public static final int StatementBlock=29;
    public static final int T__98=98;
    public static final int T__177=177;
    public static final int T__176=176;
    public static final int ABORT=43;
    public static final int T__173=173;
    public static final int StrangeNameLiteral=15;
    public static final int ONMATCH=88;
    public static final int T__172=172;
    public static final int EPLMODULE=93;
    public static final int T__175=175;
    public static final int T__174=174;
    public static final int FOR=30;
    public static final int BLOCK=62;
    public static final int T__171=171;
    public static final int T__170=170;
    public static final int PARAMETERS=46;
    public static final int SpecialNameChar=17;
    public static final int BOOLEAN=12;
    public static final int NAME=19;
    public static final int SWITCH=34;
    public static final int T__169=169;
    public static final int FeatureCall=60;
    public static final int T__122=122;
    public static final int T__121=121;
    public static final int NO=90;
    public static final int T__124=124;
    public static final int FLOAT=4;
    public static final int T__123=123;
    public static final int ACTIVE=92;
    public static final int T__120=120;
    public static final int NativeType=56;
    public static final int INT=8;
    public static final int ANNOTATIONBLOCK=50;
    public static final int RETURN=37;
    public static final int KEYVALLIST=77;
    public static final int PATTERN=82;
    public static final int NOMATCH=87;
    public static final int FEATURECALL=63;
    public static final int CollectionType=44;
    public static final int T__119=119;
    public static final int ASSIGNMENT=26;
    public static final int T__118=118;
    public static final int CARDINALITY=83;
    public static final int T__115=115;
    public static final int WS=20;
    public static final int EOF=-1;
    public static final int T__114=114;
    public static final int T__117=117;
    public static final int T__116=116;
    public static final int T__111=111;
    public static final int T__110=110;
    public static final int T__113=113;
    public static final int T__112=112;
    public static final int EscapeSequence=13;
    public static final int EOLMODULE=61;
    public static final int DIGIT=5;
    public static final int EXECUTABLEANNOTATION=51;
    public static final int T__108=108;
    public static final int T__107=107;
    public static final int WHILE=33;
    public static final int T__109=109;
    public static final int T__104=104;
    public static final int POINT=9;
    public static final int T__103=103;
    public static final int T__106=106;
    public static final int DEFAULT=36;
    public static final int MATCH=86;
    public static final int T__105=105;

    // delegates
    // delegators
    public EplParser gEpl;


        public Epl_EolParserRules(TokenStream input, EplParser gEpl) {
            this(input, new RecognizerSharedState(), gEpl);
        }
        public Epl_EolParserRules(TokenStream input, RecognizerSharedState state, EplParser gEpl) {
            super(input, state);
            this.gEpl = gEpl;
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return EplParser.tokenNames; }
    public String getGrammarFileName() { return "EolParserRules.g"; }

    
    
    public void setTokenType(ParserRuleReturnScope tree, int type) {
    	((CommonTree) tree.getTree()).getToken().setType(type);
    }
    


    public static class operationDeclarationOrAnnotationBlock_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start operationDeclarationOrAnnotationBlock
    // EolParserRules.g:106:1: operationDeclarationOrAnnotationBlock : ( operationDeclaration | annotationBlock );
    public final Epl_EolParserRules.operationDeclarationOrAnnotationBlock_return operationDeclarationOrAnnotationBlock() throws RecognitionException {
        Epl_EolParserRules.operationDeclarationOrAnnotationBlock_return retval = new Epl_EolParserRules.operationDeclarationOrAnnotationBlock_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.operationDeclaration_return operationDeclaration1 = null;

        Epl_EolParserRules.annotationBlock_return annotationBlock2 = null;



        try {
            // EolParserRules.g:107:2: ( operationDeclaration | annotationBlock )
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( ((LA1_0>=102 && LA1_0<=103)) ) {
                alt1=1;
            }
            else if ( (LA1_0==Annotation||LA1_0==108) ) {
                alt1=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }
            switch (alt1) {
                case 1 :
                    // EolParserRules.g:107:4: operationDeclaration
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_operationDeclaration_in_operationDeclarationOrAnnotationBlock259);
                    operationDeclaration1=operationDeclaration();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, operationDeclaration1.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:107:27: annotationBlock
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_annotationBlock_in_operationDeclarationOrAnnotationBlock263);
                    annotationBlock2=annotationBlock();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, annotationBlock2.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end operationDeclarationOrAnnotationBlock

    public static class modelDeclaration_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start modelDeclaration
    // EolParserRules.g:110:1: modelDeclaration : m= 'model' NAME ( modelAlias )? ( modelDriver )? ( modelDeclarationParameters )? sem= ';' ;
    public final Epl_EolParserRules.modelDeclaration_return modelDeclaration() throws RecognitionException {
        Epl_EolParserRules.modelDeclaration_return retval = new Epl_EolParserRules.modelDeclaration_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token m=null;
        Token sem=null;
        Token NAME3=null;
        Epl_EolParserRules.modelAlias_return modelAlias4 = null;

        Epl_EolParserRules.modelDriver_return modelDriver5 = null;

        Epl_EolParserRules.modelDeclarationParameters_return modelDeclarationParameters6 = null;


        org.eclipse.epsilon.common.parse.AST m_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME3_tree=null;

        try {
            // EolParserRules.g:115:2: (m= 'model' NAME ( modelAlias )? ( modelDriver )? ( modelDeclarationParameters )? sem= ';' )
            // EolParserRules.g:115:4: m= 'model' NAME ( modelAlias )? ( modelDriver )? ( modelDeclarationParameters )? sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            m=(Token)match(input,94,FOLLOW_94_in_modelDeclaration282); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            m_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(m);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(m_tree, root_0);
            }
            NAME3=(Token)match(input,NAME,FOLLOW_NAME_in_modelDeclaration285); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            NAME3_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME3);
            adaptor.addChild(root_0, NAME3_tree);
            }
            // EolParserRules.g:115:20: ( modelAlias )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==96) ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // EolParserRules.g:0:0: modelAlias
                    {
                    pushFollow(FOLLOW_modelAlias_in_modelDeclaration287);
                    modelAlias4=modelAlias();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, modelAlias4.getTree());

                    }
                    break;

            }

            // EolParserRules.g:115:32: ( modelDriver )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==98) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // EolParserRules.g:0:0: modelDriver
                    {
                    pushFollow(FOLLOW_modelDriver_in_modelDeclaration290);
                    modelDriver5=modelDriver();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, modelDriver5.getTree());

                    }
                    break;

            }

            // EolParserRules.g:115:45: ( modelDeclarationParameters )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==99) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // EolParserRules.g:0:0: modelDeclarationParameters
                    {
                    pushFollow(FOLLOW_modelDeclarationParameters_in_modelDeclaration293);
                    modelDeclarationParameters6=modelDeclarationParameters();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, modelDeclarationParameters6.getTree());

                    }
                    break;

            }

            sem=(Token)match(input,95,FOLLOW_95_in_modelDeclaration298); if (state.failed) return retval;

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getToken().setType(MODELDECLARATION);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end modelDeclaration

    public static class modelAlias_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start modelAlias
    // EolParserRules.g:118:1: modelAlias : a= 'alias' NAME ( ',' NAME )* ;
    public final Epl_EolParserRules.modelAlias_return modelAlias() throws RecognitionException {
        Epl_EolParserRules.modelAlias_return retval = new Epl_EolParserRules.modelAlias_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token a=null;
        Token NAME7=null;
        Token char_literal8=null;
        Token NAME9=null;

        org.eclipse.epsilon.common.parse.AST a_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME7_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal8_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME9_tree=null;

        try {
            // EolParserRules.g:119:2: (a= 'alias' NAME ( ',' NAME )* )
            // EolParserRules.g:119:5: a= 'alias' NAME ( ',' NAME )*
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            a=(Token)match(input,96,FOLLOW_96_in_modelAlias313); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            a_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(a);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(a_tree, root_0);
            }
            NAME7=(Token)match(input,NAME,FOLLOW_NAME_in_modelAlias316); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            NAME7_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME7);
            adaptor.addChild(root_0, NAME7_tree);
            }
            // EolParserRules.g:119:21: ( ',' NAME )*
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( (LA5_0==97) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // EolParserRules.g:119:22: ',' NAME
            	    {
            	    char_literal8=(Token)match(input,97,FOLLOW_97_in_modelAlias319); if (state.failed) return retval;
            	    NAME9=(Token)match(input,NAME,FOLLOW_NAME_in_modelAlias322); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    NAME9_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME9);
            	    adaptor.addChild(root_0, NAME9_tree);
            	    }

            	    }
            	    break;

            	default :
            	    break loop5;
                }
            } while (true);

            if ( state.backtracking==0 ) {
              a.setType(ALIAS);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end modelAlias

    public static class modelDriver_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start modelDriver
    // EolParserRules.g:123:1: modelDriver : d= 'driver' NAME ;
    public final Epl_EolParserRules.modelDriver_return modelDriver() throws RecognitionException {
        Epl_EolParserRules.modelDriver_return retval = new Epl_EolParserRules.modelDriver_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token d=null;
        Token NAME10=null;

        org.eclipse.epsilon.common.parse.AST d_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME10_tree=null;

        try {
            // EolParserRules.g:124:2: (d= 'driver' NAME )
            // EolParserRules.g:124:5: d= 'driver' NAME
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            d=(Token)match(input,98,FOLLOW_98_in_modelDriver341); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            d_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(d);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(d_tree, root_0);
            }
            NAME10=(Token)match(input,NAME,FOLLOW_NAME_in_modelDriver344); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            NAME10_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME10);
            adaptor.addChild(root_0, NAME10_tree);
            }
            if ( state.backtracking==0 ) {
              d.setType(DRIVER);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end modelDriver

    public static class modelDeclarationParameters_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start modelDeclarationParameters
    // EolParserRules.g:128:1: modelDeclarationParameters : s= '{' ( modelDeclarationParameter )? ( ',' modelDeclarationParameter )* cb= '}' ;
    public final Epl_EolParserRules.modelDeclarationParameters_return modelDeclarationParameters() throws RecognitionException {
        Epl_EolParserRules.modelDeclarationParameters_return retval = new Epl_EolParserRules.modelDeclarationParameters_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token s=null;
        Token cb=null;
        Token char_literal12=null;
        Epl_EolParserRules.modelDeclarationParameter_return modelDeclarationParameter11 = null;

        Epl_EolParserRules.modelDeclarationParameter_return modelDeclarationParameter13 = null;


        org.eclipse.epsilon.common.parse.AST s_tree=null;
        org.eclipse.epsilon.common.parse.AST cb_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal12_tree=null;

        try {
            // EolParserRules.g:133:2: (s= '{' ( modelDeclarationParameter )? ( ',' modelDeclarationParameter )* cb= '}' )
            // EolParserRules.g:133:4: s= '{' ( modelDeclarationParameter )? ( ',' modelDeclarationParameter )* cb= '}'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            s=(Token)match(input,99,FOLLOW_99_in_modelDeclarationParameters366); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            s_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(s);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(s_tree, root_0);
            }
            // EolParserRules.g:133:11: ( modelDeclarationParameter )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==NAME) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // EolParserRules.g:0:0: modelDeclarationParameter
                    {
                    pushFollow(FOLLOW_modelDeclarationParameter_in_modelDeclarationParameters369);
                    modelDeclarationParameter11=modelDeclarationParameter();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, modelDeclarationParameter11.getTree());

                    }
                    break;

            }

            // EolParserRules.g:133:38: ( ',' modelDeclarationParameter )*
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( (LA7_0==97) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // EolParserRules.g:133:39: ',' modelDeclarationParameter
            	    {
            	    char_literal12=(Token)match(input,97,FOLLOW_97_in_modelDeclarationParameters373); if (state.failed) return retval;
            	    pushFollow(FOLLOW_modelDeclarationParameter_in_modelDeclarationParameters376);
            	    modelDeclarationParameter13=modelDeclarationParameter();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, modelDeclarationParameter13.getTree());

            	    }
            	    break;

            	default :
            	    break loop7;
                }
            } while (true);

            cb=(Token)match(input,100,FOLLOW_100_in_modelDeclarationParameters382); if (state.failed) return retval;

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(cb);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getToken().setType(MODELDECLARATIONPARAMETERS);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end modelDeclarationParameters

    public static class modelDeclarationParameter_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start modelDeclarationParameter
    // EolParserRules.g:136:1: modelDeclarationParameter : NAME e= '=' STRING ;
    public final Epl_EolParserRules.modelDeclarationParameter_return modelDeclarationParameter() throws RecognitionException {
        Epl_EolParserRules.modelDeclarationParameter_return retval = new Epl_EolParserRules.modelDeclarationParameter_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token e=null;
        Token NAME14=null;
        Token STRING15=null;

        org.eclipse.epsilon.common.parse.AST e_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME14_tree=null;
        org.eclipse.epsilon.common.parse.AST STRING15_tree=null;

        try {
            // EolParserRules.g:137:2: ( NAME e= '=' STRING )
            // EolParserRules.g:137:4: NAME e= '=' STRING
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            NAME14=(Token)match(input,NAME,FOLLOW_NAME_in_modelDeclarationParameter395); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            NAME14_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME14);
            adaptor.addChild(root_0, NAME14_tree);
            }
            e=(Token)match(input,101,FOLLOW_101_in_modelDeclarationParameter399); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            e_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(e);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(e_tree, root_0);
            }
            STRING15=(Token)match(input,STRING,FOLLOW_STRING_in_modelDeclarationParameter402); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            STRING15_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(STRING15);
            adaptor.addChild(root_0, STRING15_tree);
            }
            if ( state.backtracking==0 ) {
              e.setType(MODELDECLARATIONPARAMETER);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end modelDeclarationParameter

    public static class operationDeclaration_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start operationDeclaration
    // EolParserRules.g:141:1: operationDeclaration : ( 'operation' | 'function' ) (ctx= typeName )? operationName= NAME op= '(' ( formalParameterList )? cp= ')' ( ':' returnType= typeName )? statementBlock ;
    public final Epl_EolParserRules.operationDeclaration_return operationDeclaration() throws RecognitionException {
        Epl_EolParserRules.operationDeclaration_return retval = new Epl_EolParserRules.operationDeclaration_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token operationName=null;
        Token op=null;
        Token cp=null;
        Token set16=null;
        Token char_literal18=null;
        Epl_EolParserRules.typeName_return ctx = null;

        Epl_EolParserRules.typeName_return returnType = null;

        Epl_EolParserRules.formalParameterList_return formalParameterList17 = null;

        Epl_EolParserRules.statementBlock_return statementBlock19 = null;


        org.eclipse.epsilon.common.parse.AST operationName_tree=null;
        org.eclipse.epsilon.common.parse.AST op_tree=null;
        org.eclipse.epsilon.common.parse.AST cp_tree=null;
        org.eclipse.epsilon.common.parse.AST set16_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal18_tree=null;

        try {
            // EolParserRules.g:146:2: ( ( 'operation' | 'function' ) (ctx= typeName )? operationName= NAME op= '(' ( formalParameterList )? cp= ')' ( ':' returnType= typeName )? statementBlock )
            // EolParserRules.g:146:4: ( 'operation' | 'function' ) (ctx= typeName )? operationName= NAME op= '(' ( formalParameterList )? cp= ')' ( ':' returnType= typeName )? statementBlock
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            set16=(Token)input.LT(1);
            set16=(Token)input.LT(1);
            if ( (input.LA(1)>=102 && input.LA(1)<=103) ) {
                input.consume();
                if ( state.backtracking==0 ) root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(set16), root_0);
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            // EolParserRules.g:146:30: (ctx= typeName )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==NAME) ) {
                int LA8_1 = input.LA(2);

                if ( (LA8_1==NAME||(LA8_1>=109 && LA8_1<=111)) ) {
                    alt8=1;
                }
            }
            else if ( ((LA8_0>=112 && LA8_0<=119)) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // EolParserRules.g:146:31: ctx= typeName
                    {
                    pushFollow(FOLLOW_typeName_in_operationDeclaration433);
                    ctx=typeName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, ctx.getTree());
                    if ( state.backtracking==0 ) {
                      setTokenType(ctx,TYPE);
                    }

                    }
                    break;

            }

            operationName=(Token)match(input,NAME,FOLLOW_NAME_in_operationDeclaration441); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            operationName_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(operationName);
            adaptor.addChild(root_0, operationName_tree);
            }
            op=(Token)match(input,104,FOLLOW_104_in_operationDeclaration445); if (state.failed) return retval;
            // EolParserRules.g:146:99: ( formalParameterList )?
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0==NAME) ) {
                alt9=1;
            }
            switch (alt9) {
                case 1 :
                    // EolParserRules.g:0:0: formalParameterList
                    {
                    pushFollow(FOLLOW_formalParameterList_in_operationDeclaration448);
                    formalParameterList17=formalParameterList();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, formalParameterList17.getTree());

                    }
                    break;

            }

            cp=(Token)match(input,105,FOLLOW_105_in_operationDeclaration453); if (state.failed) return retval;
            // EolParserRules.g:146:128: ( ':' returnType= typeName )?
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==106) ) {
                alt10=1;
            }
            switch (alt10) {
                case 1 :
                    // EolParserRules.g:146:129: ':' returnType= typeName
                    {
                    char_literal18=(Token)match(input,106,FOLLOW_106_in_operationDeclaration457); if (state.failed) return retval;
                    pushFollow(FOLLOW_typeName_in_operationDeclaration462);
                    returnType=typeName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, returnType.getTree());
                    if ( state.backtracking==0 ) {
                      setTokenType(returnType,TYPE);
                    }

                    }
                    break;

            }

            pushFollow(FOLLOW_statementBlock_in_operationDeclaration468);
            statementBlock19=statementBlock();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, statementBlock19.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(cp);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getToken().setType(HELPERMETHOD);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end operationDeclaration

    public static class importStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start importStatement
    // EolParserRules.g:149:1: importStatement : i= 'import' STRING sem= ';' ;
    public final Epl_EolParserRules.importStatement_return importStatement() throws RecognitionException {
        Epl_EolParserRules.importStatement_return retval = new Epl_EolParserRules.importStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token i=null;
        Token sem=null;
        Token STRING20=null;

        org.eclipse.epsilon.common.parse.AST i_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;
        org.eclipse.epsilon.common.parse.AST STRING20_tree=null;

        try {
            // EolParserRules.g:153:2: (i= 'import' STRING sem= ';' )
            // EolParserRules.g:153:4: i= 'import' STRING sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            i=(Token)match(input,107,FOLLOW_107_in_importStatement488); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            i_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(i);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(i_tree, root_0);
            }
            STRING20=(Token)match(input,STRING,FOLLOW_STRING_in_importStatement491); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            STRING20_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(STRING20);
            adaptor.addChild(root_0, STRING20_tree);
            }
            sem=(Token)match(input,95,FOLLOW_95_in_importStatement495); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              i.setType(IMPORT);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end importStatement

    public static class block_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start block
    // EolParserRules.g:157:1: block : ( statement )* -> ^( BLOCK ( statement )* ) ;
    public final Epl_EolParserRules.block_return block() throws RecognitionException {
        Epl_EolParserRules.block_return retval = new Epl_EolParserRules.block_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.statement_return statement21 = null;


        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            // EolParserRules.g:161:2: ( ( statement )* -> ^( BLOCK ( statement )* ) )
            // EolParserRules.g:161:4: ( statement )*
            {
            // EolParserRules.g:161:4: ( statement )*
            loop11:
            do {
                int alt11=2;
                int LA11_0 = input.LA(1);

                if ( (LA11_0==FLOAT||LA11_0==INT||LA11_0==BOOLEAN||LA11_0==STRING||LA11_0==NAME||LA11_0==104||(LA11_0>=112 && LA11_0<=119)||LA11_0==122||(LA11_0>=124 && LA11_0<=125)||(LA11_0>=129 && LA11_0<=137)||LA11_0==153||LA11_0==156||(LA11_0>=162 && LA11_0<=164)) ) {
                    alt11=1;
                }


                switch (alt11) {
            	case 1 :
            	    // EolParserRules.g:0:0: statement
            	    {
            	    pushFollow(FOLLOW_statement_in_block516);
            	    statement21=statement();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_statement.add(statement21.getTree());

            	    }
            	    break;

            	default :
            	    break loop11;
                }
            } while (true);



            // AST REWRITE
            // elements: statement
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
            // 162:2: -> ^( BLOCK ( statement )* )
            {
                // EolParserRules.g:162:5: ^( BLOCK ( statement )* )
                {
                org.eclipse.epsilon.common.parse.AST root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
                root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(BLOCK, "BLOCK"), root_1);

                // EolParserRules.g:162:13: ( statement )*
                while ( stream_statement.hasNext() ) {
                    adaptor.addChild(root_1, stream_statement.nextTree());

                }
                stream_statement.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).setImaginary(true);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end block

    public static class statementBlock_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start statementBlock
    // EolParserRules.g:165:1: statementBlock : s= '{' block e= '}' ;
    public final Epl_EolParserRules.statementBlock_return statementBlock() throws RecognitionException {
        Epl_EolParserRules.statementBlock_return retval = new Epl_EolParserRules.statementBlock_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token s=null;
        Token e=null;
        Epl_EolParserRules.block_return block22 = null;


        org.eclipse.epsilon.common.parse.AST s_tree=null;
        org.eclipse.epsilon.common.parse.AST e_tree=null;

        try {
            // EolParserRules.g:170:2: (s= '{' block e= '}' )
            // EolParserRules.g:170:4: s= '{' block e= '}'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            s=(Token)match(input,99,FOLLOW_99_in_statementBlock546); if (state.failed) return retval;
            pushFollow(FOLLOW_block_in_statementBlock549);
            block22=block();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, block22.getTree());
            e=(Token)match(input,100,FOLLOW_100_in_statementBlock553); if (state.failed) return retval;

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(s); 
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(e);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end statementBlock

    public static class formalParameter_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start formalParameter
    // EolParserRules.g:173:1: formalParameter : NAME ( ':' pt= typeName )? -> ^( FORMAL NAME ( typeName )? ) ;
    public final Epl_EolParserRules.formalParameter_return formalParameter() throws RecognitionException {
        Epl_EolParserRules.formalParameter_return retval = new Epl_EolParserRules.formalParameter_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token NAME23=null;
        Token char_literal24=null;
        Epl_EolParserRules.typeName_return pt = null;


        org.eclipse.epsilon.common.parse.AST NAME23_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal24_tree=null;
        RewriteRuleTokenStream stream_106=new RewriteRuleTokenStream(adaptor,"token 106");
        RewriteRuleTokenStream stream_NAME=new RewriteRuleTokenStream(adaptor,"token NAME");
        RewriteRuleSubtreeStream stream_typeName=new RewriteRuleSubtreeStream(adaptor,"rule typeName");
        try {
            // EolParserRules.g:177:2: ( NAME ( ':' pt= typeName )? -> ^( FORMAL NAME ( typeName )? ) )
            // EolParserRules.g:177:4: NAME ( ':' pt= typeName )?
            {
            NAME23=(Token)match(input,NAME,FOLLOW_NAME_in_formalParameter571); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_NAME.add(NAME23);

            // EolParserRules.g:177:9: ( ':' pt= typeName )?
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0==106) ) {
                alt12=1;
            }
            switch (alt12) {
                case 1 :
                    // EolParserRules.g:177:10: ':' pt= typeName
                    {
                    char_literal24=(Token)match(input,106,FOLLOW_106_in_formalParameter574); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_106.add(char_literal24);

                    pushFollow(FOLLOW_typeName_in_formalParameter578);
                    pt=typeName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_typeName.add(pt.getTree());
                    if ( state.backtracking==0 ) {
                      setTokenType(pt,TYPE);
                    }

                    }
                    break;

            }



            // AST REWRITE
            // elements: typeName, NAME
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
            // 178:3: -> ^( FORMAL NAME ( typeName )? )
            {
                // EolParserRules.g:178:6: ^( FORMAL NAME ( typeName )? )
                {
                org.eclipse.epsilon.common.parse.AST root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
                root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(FORMAL, "FORMAL"), root_1);

                adaptor.addChild(root_1, stream_NAME.nextNode());
                // EolParserRules.g:178:20: ( typeName )?
                if ( stream_typeName.hasNext() ) {
                    adaptor.addChild(root_1, stream_typeName.nextTree());

                }
                stream_typeName.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              	//	((org.eclipse.epsilon.common.parse.AST)retval.tree).setImaginary(true);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end formalParameter

    public static class formalParameterList_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start formalParameterList
    // EolParserRules.g:181:1: formalParameterList : formalParameter ( ',' formalParameter )* -> ^( PARAMLIST ( formalParameter )* ) ;
    public final Epl_EolParserRules.formalParameterList_return formalParameterList() throws RecognitionException {
        Epl_EolParserRules.formalParameterList_return retval = new Epl_EolParserRules.formalParameterList_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token char_literal26=null;
        Epl_EolParserRules.formalParameter_return formalParameter25 = null;

        Epl_EolParserRules.formalParameter_return formalParameter27 = null;


        org.eclipse.epsilon.common.parse.AST char_literal26_tree=null;
        RewriteRuleTokenStream stream_97=new RewriteRuleTokenStream(adaptor,"token 97");
        RewriteRuleSubtreeStream stream_formalParameter=new RewriteRuleSubtreeStream(adaptor,"rule formalParameter");
        try {
            // EolParserRules.g:185:2: ( formalParameter ( ',' formalParameter )* -> ^( PARAMLIST ( formalParameter )* ) )
            // EolParserRules.g:185:4: formalParameter ( ',' formalParameter )*
            {
            pushFollow(FOLLOW_formalParameter_in_formalParameterList612);
            formalParameter25=formalParameter();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_formalParameter.add(formalParameter25.getTree());
            // EolParserRules.g:185:20: ( ',' formalParameter )*
            loop13:
            do {
                int alt13=2;
                int LA13_0 = input.LA(1);

                if ( (LA13_0==97) ) {
                    alt13=1;
                }


                switch (alt13) {
            	case 1 :
            	    // EolParserRules.g:185:21: ',' formalParameter
            	    {
            	    char_literal26=(Token)match(input,97,FOLLOW_97_in_formalParameterList615); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_97.add(char_literal26);

            	    pushFollow(FOLLOW_formalParameter_in_formalParameterList617);
            	    formalParameter27=formalParameter();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_formalParameter.add(formalParameter27.getTree());

            	    }
            	    break;

            	default :
            	    break loop13;
                }
            } while (true);



            // AST REWRITE
            // elements: formalParameter
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
            // 186:2: -> ^( PARAMLIST ( formalParameter )* )
            {
                // EolParserRules.g:186:5: ^( PARAMLIST ( formalParameter )* )
                {
                org.eclipse.epsilon.common.parse.AST root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
                root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(PARAMLIST, "PARAMLIST"), root_1);

                // EolParserRules.g:186:17: ( formalParameter )*
                while ( stream_formalParameter.hasNext() ) {
                    adaptor.addChild(root_1, stream_formalParameter.nextTree());

                }
                stream_formalParameter.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).setImaginary(true);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end formalParameterList

    public static class executableAnnotation_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start executableAnnotation
    // EolParserRules.g:189:1: executableAnnotation : d= '$' x= . logicalExpression ;
    public final Epl_EolParserRules.executableAnnotation_return executableAnnotation() throws RecognitionException {
        Epl_EolParserRules.executableAnnotation_return retval = new Epl_EolParserRules.executableAnnotation_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token d=null;
        Token x=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression28 = null;


        org.eclipse.epsilon.common.parse.AST d_tree=null;
        org.eclipse.epsilon.common.parse.AST x_tree=null;

        try {
            // EolParserRules.g:190:2: (d= '$' x= . logicalExpression )
            // EolParserRules.g:190:4: d= '$' x= . logicalExpression
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            d=(Token)match(input,108,FOLLOW_108_in_executableAnnotation642); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            d_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(d);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(d_tree, root_0);
            }
            x=(Token)input.LT(1);
            matchAny(input); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            x_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(x);
            adaptor.addChild(root_0, x_tree);
            }
            pushFollow(FOLLOW_logicalExpression_in_executableAnnotation649);
            logicalExpression28=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression28.getTree());
            if ( state.backtracking==0 ) {
              d.setType(EXECUTABLEANNOTATION);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end executableAnnotation

    public static class annotation_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start annotation
    // EolParserRules.g:194:1: annotation : ( Annotation | executableAnnotation );
    public final Epl_EolParserRules.annotation_return annotation() throws RecognitionException {
        Epl_EolParserRules.annotation_return retval = new Epl_EolParserRules.annotation_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token Annotation29=null;
        Epl_EolParserRules.executableAnnotation_return executableAnnotation30 = null;


        org.eclipse.epsilon.common.parse.AST Annotation29_tree=null;

        try {
            // EolParserRules.g:195:2: ( Annotation | executableAnnotation )
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( (LA14_0==Annotation) ) {
                alt14=1;
            }
            else if ( (LA14_0==108) ) {
                alt14=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;
            }
            switch (alt14) {
                case 1 :
                    // EolParserRules.g:195:4: Annotation
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    Annotation29=(Token)match(input,Annotation,FOLLOW_Annotation_in_annotation663); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    Annotation29_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(Annotation29);
                    adaptor.addChild(root_0, Annotation29_tree);
                    }

                    }
                    break;
                case 2 :
                    // EolParserRules.g:195:17: executableAnnotation
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_executableAnnotation_in_annotation667);
                    executableAnnotation30=executableAnnotation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, executableAnnotation30.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end annotation

    public static class annotationBlock_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start annotationBlock
    // EolParserRules.g:198:1: annotationBlock : ( annotation )+ -> ^( ANNOTATIONBLOCK ( annotation )+ ) ;
    public final Epl_EolParserRules.annotationBlock_return annotationBlock() throws RecognitionException {
        Epl_EolParserRules.annotationBlock_return retval = new Epl_EolParserRules.annotationBlock_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.annotation_return annotation31 = null;


        RewriteRuleSubtreeStream stream_annotation=new RewriteRuleSubtreeStream(adaptor,"rule annotation");
        try {
            // EolParserRules.g:202:2: ( ( annotation )+ -> ^( ANNOTATIONBLOCK ( annotation )+ ) )
            // EolParserRules.g:202:4: ( annotation )+
            {
            // EolParserRules.g:202:4: ( annotation )+
            int cnt15=0;
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==Annotation) ) {
                    int LA15_2 = input.LA(2);

                    if ( (synpred16_EolParserRules()) ) {
                        alt15=1;
                    }


                }
                else if ( (LA15_0==108) ) {
                    int LA15_3 = input.LA(2);

                    if ( (synpred16_EolParserRules()) ) {
                        alt15=1;
                    }


                }


                switch (alt15) {
            	case 1 :
            	    // EolParserRules.g:0:0: annotation
            	    {
            	    pushFollow(FOLLOW_annotation_in_annotationBlock684);
            	    annotation31=annotation();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_annotation.add(annotation31.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt15 >= 1 ) break loop15;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(15, input);
                        throw eee;
                }
                cnt15++;
            } while (true);



            // AST REWRITE
            // elements: annotation
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
            // 203:2: -> ^( ANNOTATIONBLOCK ( annotation )+ )
            {
                // EolParserRules.g:203:5: ^( ANNOTATIONBLOCK ( annotation )+ )
                {
                org.eclipse.epsilon.common.parse.AST root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
                root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(ANNOTATIONBLOCK, "ANNOTATIONBLOCK"), root_1);

                if ( !(stream_annotation.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_annotation.hasNext() ) {
                    adaptor.addChild(root_1, stream_annotation.nextTree());

                }
                stream_annotation.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).setImaginary(true);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end annotationBlock

    public static class typeName_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start typeName
    // EolParserRules.g:206:1: typeName : ( pathName | nativeType | collectionType );
    public final Epl_EolParserRules.typeName_return typeName() throws RecognitionException {
        Epl_EolParserRules.typeName_return retval = new Epl_EolParserRules.typeName_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.pathName_return pathName32 = null;

        Epl_EolParserRules.nativeType_return nativeType33 = null;

        Epl_EolParserRules.collectionType_return collectionType34 = null;



        try {
            // EolParserRules.g:210:2: ( pathName | nativeType | collectionType )
            int alt16=3;
            switch ( input.LA(1) ) {
            case NAME:
                {
                alt16=1;
                }
                break;
            case 112:
                {
                alt16=2;
                }
                break;
            case 113:
            case 114:
            case 115:
            case 116:
            case 117:
            case 118:
            case 119:
                {
                alt16=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }

            switch (alt16) {
                case 1 :
                    // EolParserRules.g:210:4: pathName
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_pathName_in_typeName713);
                    pathName32=pathName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, pathName32.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:210:15: nativeType
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_nativeType_in_typeName717);
                    nativeType33=nativeType();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, nativeType33.getTree());

                    }
                    break;
                case 3 :
                    // EolParserRules.g:210:28: collectionType
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_collectionType_in_typeName721);
                    collectionType34=collectionType();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, collectionType34.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getToken().setType(TYPE);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end typeName

    public static class pathName_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start pathName
    // EolParserRules.g:213:1: pathName : (metamodel= NAME '!' )? head= packagedType ( '#' label= NAME )? ;
    public final Epl_EolParserRules.pathName_return pathName() throws RecognitionException {
        Epl_EolParserRules.pathName_return retval = new Epl_EolParserRules.pathName_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token metamodel=null;
        Token label=null;
        Token char_literal35=null;
        Token char_literal36=null;
        Epl_EolParserRules.packagedType_return head = null;


        org.eclipse.epsilon.common.parse.AST metamodel_tree=null;
        org.eclipse.epsilon.common.parse.AST label_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal35_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal36_tree=null;

        try {
            // EolParserRules.g:214:2: ( (metamodel= NAME '!' )? head= packagedType ( '#' label= NAME )? )
            // EolParserRules.g:214:4: (metamodel= NAME '!' )? head= packagedType ( '#' label= NAME )?
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            // EolParserRules.g:214:4: (metamodel= NAME '!' )?
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==NAME) ) {
                int LA17_1 = input.LA(2);

                if ( (LA17_1==109) ) {
                    alt17=1;
                }
            }
            switch (alt17) {
                case 1 :
                    // EolParserRules.g:214:5: metamodel= NAME '!'
                    {
                    metamodel=(Token)match(input,NAME,FOLLOW_NAME_in_pathName735); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    metamodel_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(metamodel);
                    adaptor.addChild(root_0, metamodel_tree);
                    }
                    char_literal35=(Token)match(input,109,FOLLOW_109_in_pathName737); if (state.failed) return retval;

                    }
                    break;

            }

            pushFollow(FOLLOW_packagedType_in_pathName746);
            head=packagedType();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(head.getTree(), root_0);
            // EolParserRules.g:216:3: ( '#' label= NAME )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==110) ) {
                alt18=1;
            }
            switch (alt18) {
                case 1 :
                    // EolParserRules.g:216:4: '#' label= NAME
                    {
                    char_literal36=(Token)match(input,110,FOLLOW_110_in_pathName752); if (state.failed) return retval;
                    label=(Token)match(input,NAME,FOLLOW_NAME_in_pathName757); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    label_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(label);
                    adaptor.addChild(root_0, label_tree);
                    }

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
              
              			if (metamodel != null) {
              				(head!=null?((org.eclipse.epsilon.common.parse.AST)head.tree):null).token.setText(metamodel.getText() + "!" + (head!=null?((org.eclipse.epsilon.common.parse.AST)head.tree):null).token.getText());		
              			}
              			
              			if (label != null) {
              				(head!=null?((org.eclipse.epsilon.common.parse.AST)head.tree):null).token.setText((head!=null?((org.eclipse.epsilon.common.parse.AST)head.tree):null).token.getText() + "#" + label.getText());
              				(head!=null?((org.eclipse.epsilon.common.parse.AST)head.tree):null).token.setType(ENUMERATION_VALUE);
              			}	
              		
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end pathName

    public static class packagedType_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start packagedType
    // EolParserRules.g:230:1: packagedType : head= NAME ( '::' field= NAME )* ;
    public final Epl_EolParserRules.packagedType_return packagedType() throws RecognitionException {
        Epl_EolParserRules.packagedType_return retval = new Epl_EolParserRules.packagedType_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token head=null;
        Token field=null;
        Token string_literal37=null;

        org.eclipse.epsilon.common.parse.AST head_tree=null;
        org.eclipse.epsilon.common.parse.AST field_tree=null;
        org.eclipse.epsilon.common.parse.AST string_literal37_tree=null;

        try {
            // EolParserRules.g:231:2: (head= NAME ( '::' field= NAME )* )
            // EolParserRules.g:231:4: head= NAME ( '::' field= NAME )*
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            head=(Token)match(input,NAME,FOLLOW_NAME_in_packagedType778); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            head_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(head);
            adaptor.addChild(root_0, head_tree);
            }
            // EolParserRules.g:231:14: ( '::' field= NAME )*
            loop19:
            do {
                int alt19=2;
                int LA19_0 = input.LA(1);

                if ( (LA19_0==111) ) {
                    alt19=1;
                }


                switch (alt19) {
            	case 1 :
            	    // EolParserRules.g:231:15: '::' field= NAME
            	    {
            	    string_literal37=(Token)match(input,111,FOLLOW_111_in_packagedType781); if (state.failed) return retval;
            	    field=(Token)match(input,NAME,FOLLOW_NAME_in_packagedType786); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	       
            	      				head.setText(head.getText() + "::" + field.getText()); 
            	      				((CommonToken) head).setStopIndex(((CommonToken)field).getStopIndex());
            	      			
            	    }

            	    }
            	    break;

            	default :
            	    break loop19;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end packagedType

    public static class nativeType_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start nativeType
    // EolParserRules.g:239:1: nativeType : 'Native' s= '(' STRING e= ')' ;
    public final Epl_EolParserRules.nativeType_return nativeType() throws RecognitionException {
        Epl_EolParserRules.nativeType_return retval = new Epl_EolParserRules.nativeType_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token s=null;
        Token e=null;
        Token string_literal38=null;
        Token STRING39=null;

        org.eclipse.epsilon.common.parse.AST s_tree=null;
        org.eclipse.epsilon.common.parse.AST e_tree=null;
        org.eclipse.epsilon.common.parse.AST string_literal38_tree=null;
        org.eclipse.epsilon.common.parse.AST STRING39_tree=null;

        try {
            // EolParserRules.g:245:2: ( 'Native' s= '(' STRING e= ')' )
            // EolParserRules.g:245:4: 'Native' s= '(' STRING e= ')'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            string_literal38=(Token)match(input,112,FOLLOW_112_in_nativeType815); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            string_literal38_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(string_literal38);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(string_literal38_tree, root_0);
            }
            s=(Token)match(input,104,FOLLOW_104_in_nativeType820); if (state.failed) return retval;
            STRING39=(Token)match(input,STRING,FOLLOW_STRING_in_nativeType823); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            STRING39_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(STRING39);
            adaptor.addChild(root_0, STRING39_tree);
            }
            e=(Token)match(input,105,FOLLOW_105_in_nativeType827); if (state.failed) return retval;

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(s); 
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(e);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getToken().setType(TYPE);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end nativeType

    public static class collectionType_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start collectionType
    // EolParserRules.g:248:1: collectionType : ( 'Collection' | 'Sequence' | 'List' | 'Bag' | 'Set' | 'OrderedSet' | 'Map' ) ( (op= '(' tn= typeName ( ',' tn= typeName )* cp= ')' ) | (op= '<' tn= typeName ( ',' tn= typeName )* cp= '>' ) )? ;
    public final Epl_EolParserRules.collectionType_return collectionType() throws RecognitionException {
        Epl_EolParserRules.collectionType_return retval = new Epl_EolParserRules.collectionType_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Token cp=null;
        Token set40=null;
        Token char_literal41=null;
        Token char_literal42=null;
        Epl_EolParserRules.typeName_return tn = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;
        org.eclipse.epsilon.common.parse.AST cp_tree=null;
        org.eclipse.epsilon.common.parse.AST set40_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal41_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal42_tree=null;

        try {
            // EolParserRules.g:254:2: ( ( 'Collection' | 'Sequence' | 'List' | 'Bag' | 'Set' | 'OrderedSet' | 'Map' ) ( (op= '(' tn= typeName ( ',' tn= typeName )* cp= ')' ) | (op= '<' tn= typeName ( ',' tn= typeName )* cp= '>' ) )? )
            // EolParserRules.g:254:5: ( 'Collection' | 'Sequence' | 'List' | 'Bag' | 'Set' | 'OrderedSet' | 'Map' ) ( (op= '(' tn= typeName ( ',' tn= typeName )* cp= ')' ) | (op= '<' tn= typeName ( ',' tn= typeName )* cp= '>' ) )?
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            set40=(Token)input.LT(1);
            set40=(Token)input.LT(1);
            if ( (input.LA(1)>=113 && input.LA(1)<=119) ) {
                input.consume();
                if ( state.backtracking==0 ) root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(set40), root_0);
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            // EolParserRules.g:255:3: ( (op= '(' tn= typeName ( ',' tn= typeName )* cp= ')' ) | (op= '<' tn= typeName ( ',' tn= typeName )* cp= '>' ) )?
            int alt22=3;
            alt22 = dfa22.predict(input);
            switch (alt22) {
                case 1 :
                    // EolParserRules.g:255:4: (op= '(' tn= typeName ( ',' tn= typeName )* cp= ')' )
                    {
                    // EolParserRules.g:255:4: (op= '(' tn= typeName ( ',' tn= typeName )* cp= ')' )
                    // EolParserRules.g:255:5: op= '(' tn= typeName ( ',' tn= typeName )* cp= ')'
                    {
                    op=(Token)match(input,104,FOLLOW_104_in_collectionType869); if (state.failed) return retval;
                    pushFollow(FOLLOW_typeName_in_collectionType874);
                    tn=typeName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, tn.getTree());
                    if ( state.backtracking==0 ) {
                      setTokenType(tn,TYPE);
                    }
                    // EolParserRules.g:255:50: ( ',' tn= typeName )*
                    loop20:
                    do {
                        int alt20=2;
                        int LA20_0 = input.LA(1);

                        if ( (LA20_0==97) ) {
                            alt20=1;
                        }


                        switch (alt20) {
                    	case 1 :
                    	    // EolParserRules.g:255:51: ',' tn= typeName
                    	    {
                    	    char_literal41=(Token)match(input,97,FOLLOW_97_in_collectionType879); if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) {
                    	    char_literal41_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(char_literal41);
                    	    adaptor.addChild(root_0, char_literal41_tree);
                    	    }
                    	    pushFollow(FOLLOW_typeName_in_collectionType883);
                    	    tn=typeName();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) adaptor.addChild(root_0, tn.getTree());
                    	    if ( state.backtracking==0 ) {
                    	      setTokenType(tn,TYPE);
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop20;
                        }
                    } while (true);

                    cp=(Token)match(input,105,FOLLOW_105_in_collectionType891); if (state.failed) return retval;

                    }


                    }
                    break;
                case 2 :
                    // EolParserRules.g:256:4: (op= '<' tn= typeName ( ',' tn= typeName )* cp= '>' )
                    {
                    // EolParserRules.g:256:4: (op= '<' tn= typeName ( ',' tn= typeName )* cp= '>' )
                    // EolParserRules.g:256:5: op= '<' tn= typeName ( ',' tn= typeName )* cp= '>'
                    {
                    op=(Token)match(input,120,FOLLOW_120_in_collectionType903); if (state.failed) return retval;
                    pushFollow(FOLLOW_typeName_in_collectionType908);
                    tn=typeName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, tn.getTree());
                    if ( state.backtracking==0 ) {
                      setTokenType(tn,TYPE);
                    }
                    // EolParserRules.g:256:50: ( ',' tn= typeName )*
                    loop21:
                    do {
                        int alt21=2;
                        int LA21_0 = input.LA(1);

                        if ( (LA21_0==97) ) {
                            alt21=1;
                        }


                        switch (alt21) {
                    	case 1 :
                    	    // EolParserRules.g:256:51: ',' tn= typeName
                    	    {
                    	    char_literal42=(Token)match(input,97,FOLLOW_97_in_collectionType913); if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) {
                    	    char_literal42_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(char_literal42);
                    	    adaptor.addChild(root_0, char_literal42_tree);
                    	    }
                    	    pushFollow(FOLLOW_typeName_in_collectionType917);
                    	    tn=typeName();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) adaptor.addChild(root_0, tn.getTree());
                    	    if ( state.backtracking==0 ) {
                    	      setTokenType(tn,TYPE);
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop21;
                        }
                    } while (true);

                    cp=(Token)match(input,121,FOLLOW_121_in_collectionType925); if (state.failed) return retval;

                    }


                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(op); 
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(cp);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getToken().setType(TYPE);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end collectionType

    public static class statement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start statement
    // EolParserRules.g:260:1: statement : ( statementA | statementB );
    public final Epl_EolParserRules.statement_return statement() throws RecognitionException {
        Epl_EolParserRules.statement_return retval = new Epl_EolParserRules.statement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.statementA_return statementA43 = null;

        Epl_EolParserRules.statementB_return statementB44 = null;



        try {
            // EolParserRules.g:261:2: ( statementA | statementB )
            int alt23=2;
            alt23 = dfa23.predict(input);
            switch (alt23) {
                case 1 :
                    // EolParserRules.g:261:4: statementA
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_statementA_in_statement944);
                    statementA43=statementA();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, statementA43.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:261:17: statementB
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_statementB_in_statement948);
                    statementB44=statementB();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, statementB44.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end statement

    public static class statementA_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start statementA
    // EolParserRules.g:264:1: statementA : ( assignmentStatement | expressionStatement | forStatement | ifStatement | whileStatement | switchStatement | returnStatement | breakStatement );
    public final Epl_EolParserRules.statementA_return statementA() throws RecognitionException {
        Epl_EolParserRules.statementA_return retval = new Epl_EolParserRules.statementA_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.assignmentStatement_return assignmentStatement45 = null;

        Epl_EolParserRules.expressionStatement_return expressionStatement46 = null;

        Epl_EolParserRules.forStatement_return forStatement47 = null;

        Epl_EolParserRules.ifStatement_return ifStatement48 = null;

        Epl_EolParserRules.whileStatement_return whileStatement49 = null;

        Epl_EolParserRules.switchStatement_return switchStatement50 = null;

        Epl_EolParserRules.returnStatement_return returnStatement51 = null;

        Epl_EolParserRules.breakStatement_return breakStatement52 = null;



        try {
            // EolParserRules.g:265:2: ( assignmentStatement | expressionStatement | forStatement | ifStatement | whileStatement | switchStatement | returnStatement | breakStatement )
            int alt24=8;
            alt24 = dfa24.predict(input);
            switch (alt24) {
                case 1 :
                    // EolParserRules.g:265:4: assignmentStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_assignmentStatement_in_statementA959);
                    assignmentStatement45=assignmentStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, assignmentStatement45.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:265:26: expressionStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_expressionStatement_in_statementA963);
                    expressionStatement46=expressionStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expressionStatement46.getTree());

                    }
                    break;
                case 3 :
                    // EolParserRules.g:265:48: forStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_forStatement_in_statementA967);
                    forStatement47=forStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, forStatement47.getTree());

                    }
                    break;
                case 4 :
                    // EolParserRules.g:266:5: ifStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_ifStatement_in_statementA973);
                    ifStatement48=ifStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, ifStatement48.getTree());

                    }
                    break;
                case 5 :
                    // EolParserRules.g:266:19: whileStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_whileStatement_in_statementA977);
                    whileStatement49=whileStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, whileStatement49.getTree());

                    }
                    break;
                case 6 :
                    // EolParserRules.g:266:36: switchStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_switchStatement_in_statementA981);
                    switchStatement50=switchStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, switchStatement50.getTree());

                    }
                    break;
                case 7 :
                    // EolParserRules.g:266:54: returnStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_returnStatement_in_statementA985);
                    returnStatement51=returnStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, returnStatement51.getTree());

                    }
                    break;
                case 8 :
                    // EolParserRules.g:266:72: breakStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_breakStatement_in_statementA989);
                    breakStatement52=breakStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, breakStatement52.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end statementA

    public static class statementB_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start statementB
    // EolParserRules.g:269:1: statementB : ( breakAllStatement | returnStatement | transactionStatement | abortStatement | continueStatement | throwStatement | deleteStatement );
    public final Epl_EolParserRules.statementB_return statementB() throws RecognitionException {
        Epl_EolParserRules.statementB_return retval = new Epl_EolParserRules.statementB_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.breakAllStatement_return breakAllStatement53 = null;

        Epl_EolParserRules.returnStatement_return returnStatement54 = null;

        Epl_EolParserRules.transactionStatement_return transactionStatement55 = null;

        Epl_EolParserRules.abortStatement_return abortStatement56 = null;

        Epl_EolParserRules.continueStatement_return continueStatement57 = null;

        Epl_EolParserRules.throwStatement_return throwStatement58 = null;

        Epl_EolParserRules.deleteStatement_return deleteStatement59 = null;



        try {
            // EolParserRules.g:270:2: ( breakAllStatement | returnStatement | transactionStatement | abortStatement | continueStatement | throwStatement | deleteStatement )
            int alt25=7;
            switch ( input.LA(1) ) {
            case 134:
                {
                alt25=1;
                }
                break;
            case 130:
                {
                alt25=2;
                }
                break;
            case 137:
                {
                alt25=3;
                }
                break;
            case 136:
                {
                alt25=4;
                }
                break;
            case 135:
                {
                alt25=5;
                }
                break;
            case 131:
                {
                alt25=6;
                }
                break;
            case 132:
                {
                alt25=7;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 25, 0, input);

                throw nvae;
            }

            switch (alt25) {
                case 1 :
                    // EolParserRules.g:270:4: breakAllStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_breakAllStatement_in_statementB1001);
                    breakAllStatement53=breakAllStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, breakAllStatement53.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:270:24: returnStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_returnStatement_in_statementB1005);
                    returnStatement54=returnStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, returnStatement54.getTree());

                    }
                    break;
                case 3 :
                    // EolParserRules.g:270:42: transactionStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_transactionStatement_in_statementB1009);
                    transactionStatement55=transactionStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, transactionStatement55.getTree());

                    }
                    break;
                case 4 :
                    // EolParserRules.g:271:5: abortStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_abortStatement_in_statementB1015);
                    abortStatement56=abortStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, abortStatement56.getTree());

                    }
                    break;
                case 5 :
                    // EolParserRules.g:271:22: continueStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_continueStatement_in_statementB1019);
                    continueStatement57=continueStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, continueStatement57.getTree());

                    }
                    break;
                case 6 :
                    // EolParserRules.g:271:42: throwStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_throwStatement_in_statementB1023);
                    throwStatement58=throwStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, throwStatement58.getTree());

                    }
                    break;
                case 7 :
                    // EolParserRules.g:272:5: deleteStatement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_deleteStatement_in_statementB1029);
                    deleteStatement59=deleteStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteStatement59.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end statementB

    public static class statementOrStatementBlock_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start statementOrStatementBlock
    // EolParserRules.g:275:1: statementOrStatementBlock : ( statement | statementBlock );
    public final Epl_EolParserRules.statementOrStatementBlock_return statementOrStatementBlock() throws RecognitionException {
        Epl_EolParserRules.statementOrStatementBlock_return retval = new Epl_EolParserRules.statementOrStatementBlock_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.statement_return statement60 = null;

        Epl_EolParserRules.statementBlock_return statementBlock61 = null;



        try {
            // EolParserRules.g:276:2: ( statement | statementBlock )
            int alt26=2;
            int LA26_0 = input.LA(1);

            if ( (LA26_0==FLOAT||LA26_0==INT||LA26_0==BOOLEAN||LA26_0==STRING||LA26_0==NAME||LA26_0==104||(LA26_0>=112 && LA26_0<=119)||LA26_0==122||(LA26_0>=124 && LA26_0<=125)||(LA26_0>=129 && LA26_0<=137)||LA26_0==153||LA26_0==156||(LA26_0>=162 && LA26_0<=164)) ) {
                alt26=1;
            }
            else if ( (LA26_0==99) ) {
                alt26=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 26, 0, input);

                throw nvae;
            }
            switch (alt26) {
                case 1 :
                    // EolParserRules.g:276:4: statement
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_statement_in_statementOrStatementBlock1040);
                    statement60=statement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, statement60.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:276:16: statementBlock
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_statementBlock_in_statementOrStatementBlock1044);
                    statementBlock61=statementBlock();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, statementBlock61.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end statementOrStatementBlock

    public static class expressionOrStatementBlock_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start expressionOrStatementBlock
    // EolParserRules.g:278:1: expressionOrStatementBlock : ( ':' logicalExpression | statementBlock );
    public final Epl_EolParserRules.expressionOrStatementBlock_return expressionOrStatementBlock() throws RecognitionException {
        Epl_EolParserRules.expressionOrStatementBlock_return retval = new Epl_EolParserRules.expressionOrStatementBlock_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token char_literal62=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression63 = null;

        Epl_EolParserRules.statementBlock_return statementBlock64 = null;


        org.eclipse.epsilon.common.parse.AST char_literal62_tree=null;

        try {
            // EolParserRules.g:279:2: ( ':' logicalExpression | statementBlock )
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( (LA27_0==106) ) {
                alt27=1;
            }
            else if ( (LA27_0==99) ) {
                alt27=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 27, 0, input);

                throw nvae;
            }
            switch (alt27) {
                case 1 :
                    // EolParserRules.g:279:4: ':' logicalExpression
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    char_literal62=(Token)match(input,106,FOLLOW_106_in_expressionOrStatementBlock1053); if (state.failed) return retval;
                    pushFollow(FOLLOW_logicalExpression_in_expressionOrStatementBlock1056);
                    logicalExpression63=logicalExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression63.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:279:29: statementBlock
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_statementBlock_in_expressionOrStatementBlock1060);
                    statementBlock64=statementBlock();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, statementBlock64.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end expressionOrStatementBlock

    public static class forStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start forStatement
    // EolParserRules.g:282:1: forStatement : f= 'for' '(' formalParameter 'in' logicalExpression ')' statementOrStatementBlock ;
    public final Epl_EolParserRules.forStatement_return forStatement() throws RecognitionException {
        Epl_EolParserRules.forStatement_return retval = new Epl_EolParserRules.forStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token f=null;
        Token char_literal65=null;
        Token string_literal67=null;
        Token char_literal69=null;
        Epl_EolParserRules.formalParameter_return formalParameter66 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression68 = null;

        Epl_EolParserRules.statementOrStatementBlock_return statementOrStatementBlock70 = null;


        org.eclipse.epsilon.common.parse.AST f_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal65_tree=null;
        org.eclipse.epsilon.common.parse.AST string_literal67_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal69_tree=null;

        try {
            // EolParserRules.g:283:2: (f= 'for' '(' formalParameter 'in' logicalExpression ')' statementOrStatementBlock )
            // EolParserRules.g:283:4: f= 'for' '(' formalParameter 'in' logicalExpression ')' statementOrStatementBlock
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            f=(Token)match(input,122,FOLLOW_122_in_forStatement1073); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            f_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(f);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(f_tree, root_0);
            }
            char_literal65=(Token)match(input,104,FOLLOW_104_in_forStatement1076); if (state.failed) return retval;
            pushFollow(FOLLOW_formalParameter_in_forStatement1079);
            formalParameter66=formalParameter();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, formalParameter66.getTree());
            string_literal67=(Token)match(input,123,FOLLOW_123_in_forStatement1081); if (state.failed) return retval;
            pushFollow(FOLLOW_logicalExpression_in_forStatement1084);
            logicalExpression68=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression68.getTree());
            char_literal69=(Token)match(input,105,FOLLOW_105_in_forStatement1086); if (state.failed) return retval;
            pushFollow(FOLLOW_statementOrStatementBlock_in_forStatement1089);
            statementOrStatementBlock70=statementOrStatementBlock();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, statementOrStatementBlock70.getTree());
            if ( state.backtracking==0 ) {
              f.setType(FOR);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end forStatement

    public static class ifStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start ifStatement
    // EolParserRules.g:287:1: ifStatement : i= 'if' '(' logicalExpression ')' statementOrStatementBlock ( elseStatement )? ;
    public final Epl_EolParserRules.ifStatement_return ifStatement() throws RecognitionException {
        Epl_EolParserRules.ifStatement_return retval = new Epl_EolParserRules.ifStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token i=null;
        Token char_literal71=null;
        Token char_literal73=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression72 = null;

        Epl_EolParserRules.statementOrStatementBlock_return statementOrStatementBlock74 = null;

        Epl_EolParserRules.elseStatement_return elseStatement75 = null;


        org.eclipse.epsilon.common.parse.AST i_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal71_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal73_tree=null;

        try {
            // EolParserRules.g:288:2: (i= 'if' '(' logicalExpression ')' statementOrStatementBlock ( elseStatement )? )
            // EolParserRules.g:288:4: i= 'if' '(' logicalExpression ')' statementOrStatementBlock ( elseStatement )?
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            i=(Token)match(input,124,FOLLOW_124_in_ifStatement1105); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            i_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(i);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(i_tree, root_0);
            }
            char_literal71=(Token)match(input,104,FOLLOW_104_in_ifStatement1108); if (state.failed) return retval;
            pushFollow(FOLLOW_logicalExpression_in_ifStatement1111);
            logicalExpression72=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression72.getTree());
            char_literal73=(Token)match(input,105,FOLLOW_105_in_ifStatement1113); if (state.failed) return retval;
            pushFollow(FOLLOW_statementOrStatementBlock_in_ifStatement1116);
            statementOrStatementBlock74=statementOrStatementBlock();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, statementOrStatementBlock74.getTree());
            // EolParserRules.g:288:66: ( elseStatement )?
            int alt28=2;
            int LA28_0 = input.LA(1);

            if ( (LA28_0==128) ) {
                int LA28_1 = input.LA(2);

                if ( (synpred48_EolParserRules()) ) {
                    alt28=1;
                }
            }
            switch (alt28) {
                case 1 :
                    // EolParserRules.g:0:0: elseStatement
                    {
                    pushFollow(FOLLOW_elseStatement_in_ifStatement1118);
                    elseStatement75=elseStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, elseStatement75.getTree());

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
              i.setType(IF);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end ifStatement

    public static class switchStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start switchStatement
    // EolParserRules.g:292:1: switchStatement : s= 'switch' '(' logicalExpression ')' '{' ( caseStatement )* ( defaultStatement )? '}' ;
    public final Epl_EolParserRules.switchStatement_return switchStatement() throws RecognitionException {
        Epl_EolParserRules.switchStatement_return retval = new Epl_EolParserRules.switchStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token s=null;
        Token char_literal76=null;
        Token char_literal78=null;
        Token char_literal79=null;
        Token char_literal82=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression77 = null;

        Epl_EolParserRules.caseStatement_return caseStatement80 = null;

        Epl_EolParserRules.defaultStatement_return defaultStatement81 = null;


        org.eclipse.epsilon.common.parse.AST s_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal76_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal78_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal79_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal82_tree=null;

        try {
            // EolParserRules.g:293:2: (s= 'switch' '(' logicalExpression ')' '{' ( caseStatement )* ( defaultStatement )? '}' )
            // EolParserRules.g:293:4: s= 'switch' '(' logicalExpression ')' '{' ( caseStatement )* ( defaultStatement )? '}'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            s=(Token)match(input,125,FOLLOW_125_in_switchStatement1137); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            s_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(s);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(s_tree, root_0);
            }
            char_literal76=(Token)match(input,104,FOLLOW_104_in_switchStatement1140); if (state.failed) return retval;
            pushFollow(FOLLOW_logicalExpression_in_switchStatement1143);
            logicalExpression77=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression77.getTree());
            char_literal78=(Token)match(input,105,FOLLOW_105_in_switchStatement1145); if (state.failed) return retval;
            char_literal79=(Token)match(input,99,FOLLOW_99_in_switchStatement1148); if (state.failed) return retval;
            // EolParserRules.g:293:49: ( caseStatement )*
            loop29:
            do {
                int alt29=2;
                int LA29_0 = input.LA(1);

                if ( (LA29_0==126) ) {
                    alt29=1;
                }


                switch (alt29) {
            	case 1 :
            	    // EolParserRules.g:0:0: caseStatement
            	    {
            	    pushFollow(FOLLOW_caseStatement_in_switchStatement1151);
            	    caseStatement80=caseStatement();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, caseStatement80.getTree());

            	    }
            	    break;

            	default :
            	    break loop29;
                }
            } while (true);

            // EolParserRules.g:293:64: ( defaultStatement )?
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==127) ) {
                alt30=1;
            }
            switch (alt30) {
                case 1 :
                    // EolParserRules.g:0:0: defaultStatement
                    {
                    pushFollow(FOLLOW_defaultStatement_in_switchStatement1154);
                    defaultStatement81=defaultStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, defaultStatement81.getTree());

                    }
                    break;

            }

            char_literal82=(Token)match(input,100,FOLLOW_100_in_switchStatement1157); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              s.setType(SWITCH);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end switchStatement

    public static class caseStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start caseStatement
    // EolParserRules.g:297:1: caseStatement : c= 'case' logicalExpression ':' ( block | statementBlock ) ;
    public final Epl_EolParserRules.caseStatement_return caseStatement() throws RecognitionException {
        Epl_EolParserRules.caseStatement_return retval = new Epl_EolParserRules.caseStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token c=null;
        Token char_literal84=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression83 = null;

        Epl_EolParserRules.block_return block85 = null;

        Epl_EolParserRules.statementBlock_return statementBlock86 = null;


        org.eclipse.epsilon.common.parse.AST c_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal84_tree=null;

        try {
            // EolParserRules.g:298:2: (c= 'case' logicalExpression ':' ( block | statementBlock ) )
            // EolParserRules.g:298:4: c= 'case' logicalExpression ':' ( block | statementBlock )
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            c=(Token)match(input,126,FOLLOW_126_in_caseStatement1176); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            c_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(c);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(c_tree, root_0);
            }
            pushFollow(FOLLOW_logicalExpression_in_caseStatement1179);
            logicalExpression83=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression83.getTree());
            char_literal84=(Token)match(input,106,FOLLOW_106_in_caseStatement1181); if (state.failed) return retval;
            // EolParserRules.g:298:37: ( block | statementBlock )
            int alt31=2;
            int LA31_0 = input.LA(1);

            if ( (LA31_0==EOF||LA31_0==FLOAT||LA31_0==INT||LA31_0==BOOLEAN||LA31_0==STRING||LA31_0==NAME||LA31_0==100||LA31_0==104||(LA31_0>=112 && LA31_0<=119)||LA31_0==122||(LA31_0>=124 && LA31_0<=127)||(LA31_0>=129 && LA31_0<=137)||LA31_0==153||LA31_0==156||(LA31_0>=162 && LA31_0<=164)) ) {
                alt31=1;
            }
            else if ( (LA31_0==99) ) {
                alt31=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 31, 0, input);

                throw nvae;
            }
            switch (alt31) {
                case 1 :
                    // EolParserRules.g:298:38: block
                    {
                    pushFollow(FOLLOW_block_in_caseStatement1185);
                    block85=block();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, block85.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:298:46: statementBlock
                    {
                    pushFollow(FOLLOW_statementBlock_in_caseStatement1189);
                    statementBlock86=statementBlock();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, statementBlock86.getTree());

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
              c.setType(CASE);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end caseStatement

    public static class defaultStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start defaultStatement
    // EolParserRules.g:302:1: defaultStatement : d= 'default' ':' ( block | statementBlock ) ;
    public final Epl_EolParserRules.defaultStatement_return defaultStatement() throws RecognitionException {
        Epl_EolParserRules.defaultStatement_return retval = new Epl_EolParserRules.defaultStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token d=null;
        Token char_literal87=null;
        Epl_EolParserRules.block_return block88 = null;

        Epl_EolParserRules.statementBlock_return statementBlock89 = null;


        org.eclipse.epsilon.common.parse.AST d_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal87_tree=null;

        try {
            // EolParserRules.g:303:2: (d= 'default' ':' ( block | statementBlock ) )
            // EolParserRules.g:303:4: d= 'default' ':' ( block | statementBlock )
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            d=(Token)match(input,127,FOLLOW_127_in_defaultStatement1208); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            d_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(d);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(d_tree, root_0);
            }
            char_literal87=(Token)match(input,106,FOLLOW_106_in_defaultStatement1211); if (state.failed) return retval;
            // EolParserRules.g:303:22: ( block | statementBlock )
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==EOF||LA32_0==FLOAT||LA32_0==INT||LA32_0==BOOLEAN||LA32_0==STRING||LA32_0==NAME||LA32_0==100||LA32_0==104||(LA32_0>=112 && LA32_0<=119)||LA32_0==122||(LA32_0>=124 && LA32_0<=127)||(LA32_0>=129 && LA32_0<=137)||LA32_0==153||LA32_0==156||(LA32_0>=162 && LA32_0<=164)) ) {
                alt32=1;
            }
            else if ( (LA32_0==99) ) {
                alt32=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 32, 0, input);

                throw nvae;
            }
            switch (alt32) {
                case 1 :
                    // EolParserRules.g:303:23: block
                    {
                    pushFollow(FOLLOW_block_in_defaultStatement1215);
                    block88=block();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, block88.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:303:31: statementBlock
                    {
                    pushFollow(FOLLOW_statementBlock_in_defaultStatement1219);
                    statementBlock89=statementBlock();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, statementBlock89.getTree());

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
              d.setType(DEFAULT);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end defaultStatement

    public static class elseStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start elseStatement
    // EolParserRules.g:307:1: elseStatement : e= 'else' statementOrStatementBlock ;
    public final Epl_EolParserRules.elseStatement_return elseStatement() throws RecognitionException {
        Epl_EolParserRules.elseStatement_return retval = new Epl_EolParserRules.elseStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token e=null;
        Epl_EolParserRules.statementOrStatementBlock_return statementOrStatementBlock90 = null;


        org.eclipse.epsilon.common.parse.AST e_tree=null;

        try {
            // EolParserRules.g:311:2: (e= 'else' statementOrStatementBlock )
            // EolParserRules.g:311:4: e= 'else' statementOrStatementBlock
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            e=(Token)match(input,128,FOLLOW_128_in_elseStatement1244); if (state.failed) return retval;
            pushFollow(FOLLOW_statementOrStatementBlock_in_elseStatement1247);
            statementOrStatementBlock90=statementOrStatementBlock();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, statementOrStatementBlock90.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(e);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end elseStatement

    public static class whileStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start whileStatement
    // EolParserRules.g:314:1: whileStatement : w= 'while' '(' logicalExpression ')' statementOrStatementBlock ;
    public final Epl_EolParserRules.whileStatement_return whileStatement() throws RecognitionException {
        Epl_EolParserRules.whileStatement_return retval = new Epl_EolParserRules.whileStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token w=null;
        Token char_literal91=null;
        Token char_literal93=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression92 = null;

        Epl_EolParserRules.statementOrStatementBlock_return statementOrStatementBlock94 = null;


        org.eclipse.epsilon.common.parse.AST w_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal91_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal93_tree=null;

        try {
            // EolParserRules.g:315:2: (w= 'while' '(' logicalExpression ')' statementOrStatementBlock )
            // EolParserRules.g:315:4: w= 'while' '(' logicalExpression ')' statementOrStatementBlock
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            w=(Token)match(input,129,FOLLOW_129_in_whileStatement1260); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            w_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(w);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(w_tree, root_0);
            }
            char_literal91=(Token)match(input,104,FOLLOW_104_in_whileStatement1263); if (state.failed) return retval;
            pushFollow(FOLLOW_logicalExpression_in_whileStatement1266);
            logicalExpression92=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression92.getTree());
            char_literal93=(Token)match(input,105,FOLLOW_105_in_whileStatement1268); if (state.failed) return retval;
            pushFollow(FOLLOW_statementOrStatementBlock_in_whileStatement1271);
            statementOrStatementBlock94=statementOrStatementBlock();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, statementOrStatementBlock94.getTree());
            if ( state.backtracking==0 ) {
              w.setType(WHILE);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end whileStatement

    public static class returnStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start returnStatement
    // EolParserRules.g:319:1: returnStatement : r= 'return' ( logicalExpression )? sem= ';' ;
    public final Epl_EolParserRules.returnStatement_return returnStatement() throws RecognitionException {
        Epl_EolParserRules.returnStatement_return retval = new Epl_EolParserRules.returnStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token r=null;
        Token sem=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression95 = null;


        org.eclipse.epsilon.common.parse.AST r_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:323:2: (r= 'return' ( logicalExpression )? sem= ';' )
            // EolParserRules.g:323:4: r= 'return' ( logicalExpression )? sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            r=(Token)match(input,130,FOLLOW_130_in_returnStatement1293); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            r_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(r);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(r_tree, root_0);
            }
            // EolParserRules.g:323:16: ( logicalExpression )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==FLOAT||LA33_0==INT||LA33_0==BOOLEAN||LA33_0==STRING||LA33_0==NAME||LA33_0==104||(LA33_0>=112 && LA33_0<=119)||LA33_0==153||LA33_0==156||(LA33_0>=162 && LA33_0<=164)) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // EolParserRules.g:0:0: logicalExpression
                    {
                    pushFollow(FOLLOW_logicalExpression_in_returnStatement1296);
                    logicalExpression95=logicalExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression95.getTree());

                    }
                    break;

            }

            sem=(Token)match(input,95,FOLLOW_95_in_returnStatement1301); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              r.setType(RETURN);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end returnStatement

    public static class throwStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start throwStatement
    // EolParserRules.g:327:1: throwStatement : t= 'throw' ( logicalExpression )? sem= ';' ;
    public final Epl_EolParserRules.throwStatement_return throwStatement() throws RecognitionException {
        Epl_EolParserRules.throwStatement_return retval = new Epl_EolParserRules.throwStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token t=null;
        Token sem=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression96 = null;


        org.eclipse.epsilon.common.parse.AST t_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:331:2: (t= 'throw' ( logicalExpression )? sem= ';' )
            // EolParserRules.g:331:4: t= 'throw' ( logicalExpression )? sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            t=(Token)match(input,131,FOLLOW_131_in_throwStatement1324); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            t_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(t);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(t_tree, root_0);
            }
            // EolParserRules.g:331:15: ( logicalExpression )?
            int alt34=2;
            int LA34_0 = input.LA(1);

            if ( (LA34_0==FLOAT||LA34_0==INT||LA34_0==BOOLEAN||LA34_0==STRING||LA34_0==NAME||LA34_0==104||(LA34_0>=112 && LA34_0<=119)||LA34_0==153||LA34_0==156||(LA34_0>=162 && LA34_0<=164)) ) {
                alt34=1;
            }
            switch (alt34) {
                case 1 :
                    // EolParserRules.g:0:0: logicalExpression
                    {
                    pushFollow(FOLLOW_logicalExpression_in_throwStatement1327);
                    logicalExpression96=logicalExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression96.getTree());

                    }
                    break;

            }

            sem=(Token)match(input,95,FOLLOW_95_in_throwStatement1332); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              t.setType(THROW);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end throwStatement

    public static class deleteStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start deleteStatement
    // EolParserRules.g:335:1: deleteStatement : d= 'delete' ( logicalExpression )? sem= ';' ;
    public final Epl_EolParserRules.deleteStatement_return deleteStatement() throws RecognitionException {
        Epl_EolParserRules.deleteStatement_return retval = new Epl_EolParserRules.deleteStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token d=null;
        Token sem=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression97 = null;


        org.eclipse.epsilon.common.parse.AST d_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:339:2: (d= 'delete' ( logicalExpression )? sem= ';' )
            // EolParserRules.g:339:4: d= 'delete' ( logicalExpression )? sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            d=(Token)match(input,132,FOLLOW_132_in_deleteStatement1355); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            d_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(d);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(d_tree, root_0);
            }
            // EolParserRules.g:339:16: ( logicalExpression )?
            int alt35=2;
            int LA35_0 = input.LA(1);

            if ( (LA35_0==FLOAT||LA35_0==INT||LA35_0==BOOLEAN||LA35_0==STRING||LA35_0==NAME||LA35_0==104||(LA35_0>=112 && LA35_0<=119)||LA35_0==153||LA35_0==156||(LA35_0>=162 && LA35_0<=164)) ) {
                alt35=1;
            }
            switch (alt35) {
                case 1 :
                    // EolParserRules.g:0:0: logicalExpression
                    {
                    pushFollow(FOLLOW_logicalExpression_in_deleteStatement1358);
                    logicalExpression97=logicalExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression97.getTree());

                    }
                    break;

            }

            sem=(Token)match(input,95,FOLLOW_95_in_deleteStatement1363); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              d.setType(DELETE);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end deleteStatement

    public static class breakStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start breakStatement
    // EolParserRules.g:343:1: breakStatement : b= 'break' sem= ';' ;
    public final Epl_EolParserRules.breakStatement_return breakStatement() throws RecognitionException {
        Epl_EolParserRules.breakStatement_return retval = new Epl_EolParserRules.breakStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token b=null;
        Token sem=null;

        org.eclipse.epsilon.common.parse.AST b_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:347:2: (b= 'break' sem= ';' )
            // EolParserRules.g:347:4: b= 'break' sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            b=(Token)match(input,133,FOLLOW_133_in_breakStatement1389); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            b_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(b);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(b_tree, root_0);
            }
            sem=(Token)match(input,95,FOLLOW_95_in_breakStatement1394); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              b.setType(BREAK);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end breakStatement

    public static class breakAllStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start breakAllStatement
    // EolParserRules.g:351:1: breakAllStatement : b= 'breakAll' sem= ';' ;
    public final Epl_EolParserRules.breakAllStatement_return breakAllStatement() throws RecognitionException {
        Epl_EolParserRules.breakAllStatement_return retval = new Epl_EolParserRules.breakAllStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token b=null;
        Token sem=null;

        org.eclipse.epsilon.common.parse.AST b_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:355:2: (b= 'breakAll' sem= ';' )
            // EolParserRules.g:355:4: b= 'breakAll' sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            b=(Token)match(input,134,FOLLOW_134_in_breakAllStatement1417); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            b_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(b);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(b_tree, root_0);
            }
            sem=(Token)match(input,95,FOLLOW_95_in_breakAllStatement1422); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              b.setType(BREAKALL);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end breakAllStatement

    public static class continueStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start continueStatement
    // EolParserRules.g:359:1: continueStatement : c= 'continue' sem= ';' ;
    public final Epl_EolParserRules.continueStatement_return continueStatement() throws RecognitionException {
        Epl_EolParserRules.continueStatement_return retval = new Epl_EolParserRules.continueStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token c=null;
        Token sem=null;

        org.eclipse.epsilon.common.parse.AST c_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:363:2: (c= 'continue' sem= ';' )
            // EolParserRules.g:363:4: c= 'continue' sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            c=(Token)match(input,135,FOLLOW_135_in_continueStatement1445); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            c_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(c);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(c_tree, root_0);
            }
            sem=(Token)match(input,95,FOLLOW_95_in_continueStatement1450); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              c.setType(CONTINUE);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end continueStatement

    public static class abortStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start abortStatement
    // EolParserRules.g:367:1: abortStatement : a= 'abort' sem= ';' ;
    public final Epl_EolParserRules.abortStatement_return abortStatement() throws RecognitionException {
        Epl_EolParserRules.abortStatement_return retval = new Epl_EolParserRules.abortStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token a=null;
        Token sem=null;

        org.eclipse.epsilon.common.parse.AST a_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:371:2: (a= 'abort' sem= ';' )
            // EolParserRules.g:371:4: a= 'abort' sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            a=(Token)match(input,136,FOLLOW_136_in_abortStatement1473); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            a_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(a);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(a_tree, root_0);
            }
            sem=(Token)match(input,95,FOLLOW_95_in_abortStatement1478); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              a.setType(ABORT);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end abortStatement

    public static class transactionStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start transactionStatement
    // EolParserRules.g:375:1: transactionStatement : t= 'transaction' ( NAME ( ',' NAME )* )? statementOrStatementBlock ;
    public final Epl_EolParserRules.transactionStatement_return transactionStatement() throws RecognitionException {
        Epl_EolParserRules.transactionStatement_return retval = new Epl_EolParserRules.transactionStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token t=null;
        Token NAME98=null;
        Token char_literal99=null;
        Token NAME100=null;
        Epl_EolParserRules.statementOrStatementBlock_return statementOrStatementBlock101 = null;


        org.eclipse.epsilon.common.parse.AST t_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME98_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal99_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME100_tree=null;

        try {
            // EolParserRules.g:376:2: (t= 'transaction' ( NAME ( ',' NAME )* )? statementOrStatementBlock )
            // EolParserRules.g:376:4: t= 'transaction' ( NAME ( ',' NAME )* )? statementOrStatementBlock
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            t=(Token)match(input,137,FOLLOW_137_in_transactionStatement1495); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            t_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(t);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(t_tree, root_0);
            }
            // EolParserRules.g:376:21: ( NAME ( ',' NAME )* )?
            int alt37=2;
            alt37 = dfa37.predict(input);
            switch (alt37) {
                case 1 :
                    // EolParserRules.g:376:22: NAME ( ',' NAME )*
                    {
                    NAME98=(Token)match(input,NAME,FOLLOW_NAME_in_transactionStatement1499); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    NAME98_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME98);
                    adaptor.addChild(root_0, NAME98_tree);
                    }
                    // EolParserRules.g:376:27: ( ',' NAME )*
                    loop36:
                    do {
                        int alt36=2;
                        int LA36_0 = input.LA(1);

                        if ( (LA36_0==97) ) {
                            alt36=1;
                        }


                        switch (alt36) {
                    	case 1 :
                    	    // EolParserRules.g:376:28: ',' NAME
                    	    {
                    	    char_literal99=(Token)match(input,97,FOLLOW_97_in_transactionStatement1502); if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) {
                    	    char_literal99_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(char_literal99);
                    	    adaptor.addChild(root_0, char_literal99_tree);
                    	    }
                    	    NAME100=(Token)match(input,NAME,FOLLOW_NAME_in_transactionStatement1504); if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) {
                    	    NAME100_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME100);
                    	    adaptor.addChild(root_0, NAME100_tree);
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop36;
                        }
                    } while (true);


                    }
                    break;

            }

            pushFollow(FOLLOW_statementOrStatementBlock_in_transactionStatement1510);
            statementOrStatementBlock101=statementOrStatementBlock();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, statementOrStatementBlock101.getTree());
            if ( state.backtracking==0 ) {
              t.setType(TRANSACTION);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end transactionStatement

    public static class assignmentStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start assignmentStatement
    // EolParserRules.g:380:1: assignmentStatement : logicalExpression ( (normal= ':=' | normal= '+=' | normal= '-=' | normal= '*=' | normal= '/=' ) | special= '::=' ) logicalExpression sem= ';' ;
    public final Epl_EolParserRules.assignmentStatement_return assignmentStatement() throws RecognitionException {
        Epl_EolParserRules.assignmentStatement_return retval = new Epl_EolParserRules.assignmentStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token normal=null;
        Token special=null;
        Token sem=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression102 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression103 = null;


        org.eclipse.epsilon.common.parse.AST normal_tree=null;
        org.eclipse.epsilon.common.parse.AST special_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:384:2: ( logicalExpression ( (normal= ':=' | normal= '+=' | normal= '-=' | normal= '*=' | normal= '/=' ) | special= '::=' ) logicalExpression sem= ';' )
            // EolParserRules.g:384:4: logicalExpression ( (normal= ':=' | normal= '+=' | normal= '-=' | normal= '*=' | normal= '/=' ) | special= '::=' ) logicalExpression sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_logicalExpression_in_assignmentStatement1530);
            logicalExpression102=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression102.getTree());
            // EolParserRules.g:384:22: ( (normal= ':=' | normal= '+=' | normal= '-=' | normal= '*=' | normal= '/=' ) | special= '::=' )
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( ((LA39_0>=138 && LA39_0<=142)) ) {
                alt39=1;
            }
            else if ( (LA39_0==143) ) {
                alt39=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 39, 0, input);

                throw nvae;
            }
            switch (alt39) {
                case 1 :
                    // EolParserRules.g:384:23: (normal= ':=' | normal= '+=' | normal= '-=' | normal= '*=' | normal= '/=' )
                    {
                    // EolParserRules.g:384:23: (normal= ':=' | normal= '+=' | normal= '-=' | normal= '*=' | normal= '/=' )
                    int alt38=5;
                    switch ( input.LA(1) ) {
                    case 138:
                        {
                        alt38=1;
                        }
                        break;
                    case 139:
                        {
                        alt38=2;
                        }
                        break;
                    case 140:
                        {
                        alt38=3;
                        }
                        break;
                    case 141:
                        {
                        alt38=4;
                        }
                        break;
                    case 142:
                        {
                        alt38=5;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 38, 0, input);

                        throw nvae;
                    }

                    switch (alt38) {
                        case 1 :
                            // EolParserRules.g:384:24: normal= ':='
                            {
                            normal=(Token)match(input,138,FOLLOW_138_in_assignmentStatement1536); if (state.failed) return retval;
                            if ( state.backtracking==0 ) {
                            normal_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(normal);
                            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(normal_tree, root_0);
                            }

                            }
                            break;
                        case 2 :
                            // EolParserRules.g:384:37: normal= '+='
                            {
                            normal=(Token)match(input,139,FOLLOW_139_in_assignmentStatement1541); if (state.failed) return retval;
                            if ( state.backtracking==0 ) {
                            normal_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(normal);
                            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(normal_tree, root_0);
                            }

                            }
                            break;
                        case 3 :
                            // EolParserRules.g:384:50: normal= '-='
                            {
                            normal=(Token)match(input,140,FOLLOW_140_in_assignmentStatement1546); if (state.failed) return retval;
                            if ( state.backtracking==0 ) {
                            normal_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(normal);
                            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(normal_tree, root_0);
                            }

                            }
                            break;
                        case 4 :
                            // EolParserRules.g:384:63: normal= '*='
                            {
                            normal=(Token)match(input,141,FOLLOW_141_in_assignmentStatement1551); if (state.failed) return retval;
                            if ( state.backtracking==0 ) {
                            normal_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(normal);
                            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(normal_tree, root_0);
                            }

                            }
                            break;
                        case 5 :
                            // EolParserRules.g:384:76: normal= '/='
                            {
                            normal=(Token)match(input,142,FOLLOW_142_in_assignmentStatement1556); if (state.failed) return retval;
                            if ( state.backtracking==0 ) {
                            normal_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(normal);
                            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(normal_tree, root_0);
                            }

                            }
                            break;

                    }

                    if ( state.backtracking==0 ) {
                      normal.setType(ASSIGNMENT);
                    }

                    }
                    break;
                case 2 :
                    // EolParserRules.g:384:120: special= '::='
                    {
                    special=(Token)match(input,143,FOLLOW_143_in_assignmentStatement1564); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    special_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(special);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(special_tree, root_0);
                    }
                    if ( state.backtracking==0 ) {
                      special.setType(SPECIAL_ASSIGNMENT);
                    }

                    }
                    break;

            }

            pushFollow(FOLLOW_logicalExpression_in_assignmentStatement1570);
            logicalExpression103=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression103.getTree());
            sem=(Token)match(input,95,FOLLOW_95_in_assignmentStatement1574); if (state.failed) return retval;

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end assignmentStatement

    public static class expressionStatement_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start expressionStatement
    // EolParserRules.g:388:1: expressionStatement : ( postfixExpression op= '=' logicalExpression | logicalExpression ) sem= ';' ;
    public final Epl_EolParserRules.expressionStatement_return expressionStatement() throws RecognitionException {
        Epl_EolParserRules.expressionStatement_return retval = new Epl_EolParserRules.expressionStatement_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Token sem=null;
        Epl_EolParserRules.postfixExpression_return postfixExpression104 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression105 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression106 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;
        org.eclipse.epsilon.common.parse.AST sem_tree=null;

        try {
            // EolParserRules.g:392:2: ( ( postfixExpression op= '=' logicalExpression | logicalExpression ) sem= ';' )
            // EolParserRules.g:392:4: ( postfixExpression op= '=' logicalExpression | logicalExpression ) sem= ';'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            // EolParserRules.g:392:4: ( postfixExpression op= '=' logicalExpression | logicalExpression )
            int alt40=2;
            alt40 = dfa40.predict(input);
            switch (alt40) {
                case 1 :
                    // EolParserRules.g:392:5: postfixExpression op= '=' logicalExpression
                    {
                    pushFollow(FOLLOW_postfixExpression_in_expressionStatement1596);
                    postfixExpression104=postfixExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, postfixExpression104.getTree());
                    op=(Token)match(input,101,FOLLOW_101_in_expressionStatement1600); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
                    }
                    pushFollow(FOLLOW_logicalExpression_in_expressionStatement1603);
                    logicalExpression105=logicalExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression105.getTree());
                    if ( state.backtracking==0 ) {
                      op.setType(OPERATOR);
                    }

                    }
                    break;
                case 2 :
                    // EolParserRules.g:392:76: logicalExpression
                    {
                    pushFollow(FOLLOW_logicalExpression_in_expressionStatement1609);
                    logicalExpression106=logicalExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression106.getTree());

                    }
                    break;

            }

            sem=(Token)match(input,95,FOLLOW_95_in_expressionStatement1614); if (state.failed) return retval;

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(sem);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end expressionStatement

    public static class logicalExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start logicalExpression
    // EolParserRules.g:395:1: logicalExpression : relationalExpression ( (op= 'or' | op= 'and' | op= 'xor' | op= 'implies' ) relationalExpression )* ;
    public final Epl_EolParserRules.logicalExpression_return logicalExpression() throws RecognitionException {
        Epl_EolParserRules.logicalExpression_return retval = new Epl_EolParserRules.logicalExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Epl_EolParserRules.relationalExpression_return relationalExpression107 = null;

        Epl_EolParserRules.relationalExpression_return relationalExpression108 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;

        try {
            // EolParserRules.g:396:2: ( relationalExpression ( (op= 'or' | op= 'and' | op= 'xor' | op= 'implies' ) relationalExpression )* )
            // EolParserRules.g:396:4: relationalExpression ( (op= 'or' | op= 'and' | op= 'xor' | op= 'implies' ) relationalExpression )*
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_relationalExpression_in_logicalExpression1626);
            relationalExpression107=relationalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, relationalExpression107.getTree());
            // EolParserRules.g:396:25: ( (op= 'or' | op= 'and' | op= 'xor' | op= 'implies' ) relationalExpression )*
            loop42:
            do {
                int alt42=2;
                int LA42_0 = input.LA(1);

                if ( ((LA42_0>=144 && LA42_0<=147)) ) {
                    alt42=1;
                }


                switch (alt42) {
            	case 1 :
            	    // EolParserRules.g:396:26: (op= 'or' | op= 'and' | op= 'xor' | op= 'implies' ) relationalExpression
            	    {
            	    // EolParserRules.g:396:26: (op= 'or' | op= 'and' | op= 'xor' | op= 'implies' )
            	    int alt41=4;
            	    switch ( input.LA(1) ) {
            	    case 144:
            	        {
            	        alt41=1;
            	        }
            	        break;
            	    case 145:
            	        {
            	        alt41=2;
            	        }
            	        break;
            	    case 146:
            	        {
            	        alt41=3;
            	        }
            	        break;
            	    case 147:
            	        {
            	        alt41=4;
            	        }
            	        break;
            	    default:
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 41, 0, input);

            	        throw nvae;
            	    }

            	    switch (alt41) {
            	        case 1 :
            	            // EolParserRules.g:396:27: op= 'or'
            	            {
            	            op=(Token)match(input,144,FOLLOW_144_in_logicalExpression1632); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 2 :
            	            // EolParserRules.g:396:36: op= 'and'
            	            {
            	            op=(Token)match(input,145,FOLLOW_145_in_logicalExpression1637); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 3 :
            	            // EolParserRules.g:396:46: op= 'xor'
            	            {
            	            op=(Token)match(input,146,FOLLOW_146_in_logicalExpression1642); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 4 :
            	            // EolParserRules.g:396:56: op= 'implies'
            	            {
            	            op=(Token)match(input,147,FOLLOW_147_in_logicalExpression1647); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_relationalExpression_in_logicalExpression1651);
            	    relationalExpression108=relationalExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, relationalExpression108.getTree());
            	    if ( state.backtracking==0 ) {
            	      op.setType(OPERATOR);
            	    }

            	    }
            	    break;

            	default :
            	    break loop42;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end logicalExpression

    public static class relationalExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start relationalExpression
    // EolParserRules.g:400:1: relationalExpression : additiveExpression ( (op= '==' relationalExpression | op= '=' relationalExpression | (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' ) additiveExpression ) )* ;
    public final Epl_EolParserRules.relationalExpression_return relationalExpression() throws RecognitionException {
        Epl_EolParserRules.relationalExpression_return retval = new Epl_EolParserRules.relationalExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Epl_EolParserRules.additiveExpression_return additiveExpression109 = null;

        Epl_EolParserRules.relationalExpression_return relationalExpression110 = null;

        Epl_EolParserRules.relationalExpression_return relationalExpression111 = null;

        Epl_EolParserRules.additiveExpression_return additiveExpression112 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;

        try {
            // EolParserRules.g:401:2: ( additiveExpression ( (op= '==' relationalExpression | op= '=' relationalExpression | (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' ) additiveExpression ) )* )
            // EolParserRules.g:401:4: additiveExpression ( (op= '==' relationalExpression | op= '=' relationalExpression | (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' ) additiveExpression ) )*
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_additiveExpression_in_relationalExpression1668);
            additiveExpression109=additiveExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, additiveExpression109.getTree());
            // EolParserRules.g:401:23: ( (op= '==' relationalExpression | op= '=' relationalExpression | (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' ) additiveExpression ) )*
            loop45:
            do {
                int alt45=2;
                alt45 = dfa45.predict(input);
                switch (alt45) {
            	case 1 :
            	    // EolParserRules.g:401:24: (op= '==' relationalExpression | op= '=' relationalExpression | (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' ) additiveExpression )
            	    {
            	    // EolParserRules.g:401:24: (op= '==' relationalExpression | op= '=' relationalExpression | (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' ) additiveExpression )
            	    int alt44=3;
            	    switch ( input.LA(1) ) {
            	    case 148:
            	        {
            	        alt44=1;
            	        }
            	        break;
            	    case 101:
            	        {
            	        alt44=2;
            	        }
            	        break;
            	    case 120:
            	    case 121:
            	    case 149:
            	    case 150:
            	    case 151:
            	        {
            	        alt44=3;
            	        }
            	        break;
            	    default:
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 44, 0, input);

            	        throw nvae;
            	    }

            	    switch (alt44) {
            	        case 1 :
            	            // EolParserRules.g:401:25: op= '==' relationalExpression
            	            {
            	            op=(Token)match(input,148,FOLLOW_148_in_relationalExpression1674); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }
            	            pushFollow(FOLLOW_relationalExpression_in_relationalExpression1677);
            	            relationalExpression110=relationalExpression();

            	            state._fsp--;
            	            if (state.failed) return retval;
            	            if ( state.backtracking==0 ) adaptor.addChild(root_0, relationalExpression110.getTree());

            	            }
            	            break;
            	        case 2 :
            	            // EolParserRules.g:401:57: op= '=' relationalExpression
            	            {
            	            op=(Token)match(input,101,FOLLOW_101_in_relationalExpression1683); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }
            	            pushFollow(FOLLOW_relationalExpression_in_relationalExpression1686);
            	            relationalExpression111=relationalExpression();

            	            state._fsp--;
            	            if (state.failed) return retval;
            	            if ( state.backtracking==0 ) adaptor.addChild(root_0, relationalExpression111.getTree());

            	            }
            	            break;
            	        case 3 :
            	            // EolParserRules.g:402:24: (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' ) additiveExpression
            	            {
            	            // EolParserRules.g:402:24: (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' )
            	            int alt43=5;
            	            switch ( input.LA(1) ) {
            	            case 121:
            	                {
            	                alt43=1;
            	                }
            	                break;
            	            case 120:
            	                {
            	                alt43=2;
            	                }
            	                break;
            	            case 149:
            	                {
            	                alt43=3;
            	                }
            	                break;
            	            case 150:
            	                {
            	                alt43=4;
            	                }
            	                break;
            	            case 151:
            	                {
            	                alt43=5;
            	                }
            	                break;
            	            default:
            	                if (state.backtracking>0) {state.failed=true; return retval;}
            	                NoViableAltException nvae =
            	                    new NoViableAltException("", 43, 0, input);

            	                throw nvae;
            	            }

            	            switch (alt43) {
            	                case 1 :
            	                    // EolParserRules.g:402:25: op= '>'
            	                    {
            	                    op=(Token)match(input,121,FOLLOW_121_in_relationalExpression1716); if (state.failed) return retval;
            	                    if ( state.backtracking==0 ) {
            	                    op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	                    }

            	                    }
            	                    break;
            	                case 2 :
            	                    // EolParserRules.g:402:33: op= '<'
            	                    {
            	                    op=(Token)match(input,120,FOLLOW_120_in_relationalExpression1721); if (state.failed) return retval;
            	                    if ( state.backtracking==0 ) {
            	                    op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	                    }

            	                    }
            	                    break;
            	                case 3 :
            	                    // EolParserRules.g:402:41: op= '>='
            	                    {
            	                    op=(Token)match(input,149,FOLLOW_149_in_relationalExpression1726); if (state.failed) return retval;
            	                    if ( state.backtracking==0 ) {
            	                    op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	                    }

            	                    }
            	                    break;
            	                case 4 :
            	                    // EolParserRules.g:402:50: op= '<='
            	                    {
            	                    op=(Token)match(input,150,FOLLOW_150_in_relationalExpression1731); if (state.failed) return retval;
            	                    if ( state.backtracking==0 ) {
            	                    op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	                    }

            	                    }
            	                    break;
            	                case 5 :
            	                    // EolParserRules.g:402:59: op= '<>'
            	                    {
            	                    op=(Token)match(input,151,FOLLOW_151_in_relationalExpression1736); if (state.failed) return retval;
            	                    if ( state.backtracking==0 ) {
            	                    op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	                    }

            	                    }
            	                    break;

            	            }

            	            pushFollow(FOLLOW_additiveExpression_in_relationalExpression1740);
            	            additiveExpression112=additiveExpression();

            	            state._fsp--;
            	            if (state.failed) return retval;
            	            if ( state.backtracking==0 ) adaptor.addChild(root_0, additiveExpression112.getTree());

            	            }
            	            break;

            	    }

            	    if ( state.backtracking==0 ) {
            	      op.setType(OPERATOR);
            	    }

            	    }
            	    break;

            	default :
            	    break loop45;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end relationalExpression

    public static class additiveExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start additiveExpression
    // EolParserRules.g:406:1: additiveExpression : multiplicativeExpression ( (op= '+' | op= '-' ) multiplicativeExpression )* ;
    public final Epl_EolParserRules.additiveExpression_return additiveExpression() throws RecognitionException {
        Epl_EolParserRules.additiveExpression_return retval = new Epl_EolParserRules.additiveExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Epl_EolParserRules.multiplicativeExpression_return multiplicativeExpression113 = null;

        Epl_EolParserRules.multiplicativeExpression_return multiplicativeExpression114 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;

        try {
            // EolParserRules.g:407:2: ( multiplicativeExpression ( (op= '+' | op= '-' ) multiplicativeExpression )* )
            // EolParserRules.g:407:4: multiplicativeExpression ( (op= '+' | op= '-' ) multiplicativeExpression )*
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_multiplicativeExpression_in_additiveExpression1758);
            multiplicativeExpression113=multiplicativeExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, multiplicativeExpression113.getTree());
            // EolParserRules.g:407:29: ( (op= '+' | op= '-' ) multiplicativeExpression )*
            loop47:
            do {
                int alt47=2;
                int LA47_0 = input.LA(1);

                if ( ((LA47_0>=152 && LA47_0<=153)) ) {
                    alt47=1;
                }


                switch (alt47) {
            	case 1 :
            	    // EolParserRules.g:407:30: (op= '+' | op= '-' ) multiplicativeExpression
            	    {
            	    // EolParserRules.g:407:30: (op= '+' | op= '-' )
            	    int alt46=2;
            	    int LA46_0 = input.LA(1);

            	    if ( (LA46_0==152) ) {
            	        alt46=1;
            	    }
            	    else if ( (LA46_0==153) ) {
            	        alt46=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 46, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt46) {
            	        case 1 :
            	            // EolParserRules.g:407:31: op= '+'
            	            {
            	            op=(Token)match(input,152,FOLLOW_152_in_additiveExpression1764); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 2 :
            	            // EolParserRules.g:407:39: op= '-'
            	            {
            	            op=(Token)match(input,153,FOLLOW_153_in_additiveExpression1769); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_multiplicativeExpression_in_additiveExpression1773);
            	    multiplicativeExpression114=multiplicativeExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, multiplicativeExpression114.getTree());
            	    if ( state.backtracking==0 ) {
            	      op.setType(OPERATOR);
            	    }

            	    }
            	    break;

            	default :
            	    break loop47;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end additiveExpression

    public static class multiplicativeExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start multiplicativeExpression
    // EolParserRules.g:411:1: multiplicativeExpression : unaryExpression ( (op= '*' | op= '/' ) unaryExpression )* ;
    public final Epl_EolParserRules.multiplicativeExpression_return multiplicativeExpression() throws RecognitionException {
        Epl_EolParserRules.multiplicativeExpression_return retval = new Epl_EolParserRules.multiplicativeExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Epl_EolParserRules.unaryExpression_return unaryExpression115 = null;

        Epl_EolParserRules.unaryExpression_return unaryExpression116 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;

        try {
            // EolParserRules.g:412:2: ( unaryExpression ( (op= '*' | op= '/' ) unaryExpression )* )
            // EolParserRules.g:412:4: unaryExpression ( (op= '*' | op= '/' ) unaryExpression )*
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_unaryExpression_in_multiplicativeExpression1791);
            unaryExpression115=unaryExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, unaryExpression115.getTree());
            // EolParserRules.g:412:20: ( (op= '*' | op= '/' ) unaryExpression )*
            loop49:
            do {
                int alt49=2;
                int LA49_0 = input.LA(1);

                if ( ((LA49_0>=154 && LA49_0<=155)) ) {
                    alt49=1;
                }


                switch (alt49) {
            	case 1 :
            	    // EolParserRules.g:412:21: (op= '*' | op= '/' ) unaryExpression
            	    {
            	    // EolParserRules.g:412:21: (op= '*' | op= '/' )
            	    int alt48=2;
            	    int LA48_0 = input.LA(1);

            	    if ( (LA48_0==154) ) {
            	        alt48=1;
            	    }
            	    else if ( (LA48_0==155) ) {
            	        alt48=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 48, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt48) {
            	        case 1 :
            	            // EolParserRules.g:412:22: op= '*'
            	            {
            	            op=(Token)match(input,154,FOLLOW_154_in_multiplicativeExpression1797); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 2 :
            	            // EolParserRules.g:412:30: op= '/'
            	            {
            	            op=(Token)match(input,155,FOLLOW_155_in_multiplicativeExpression1802); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
            	            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
            	            }

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_unaryExpression_in_multiplicativeExpression1806);
            	    unaryExpression116=unaryExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, unaryExpression116.getTree());
            	    if ( state.backtracking==0 ) {
            	      op.setType(OPERATOR);
            	    }

            	    }
            	    break;

            	default :
            	    break loop49;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end multiplicativeExpression

    public static class unaryExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start unaryExpression
    // EolParserRules.g:416:1: unaryExpression : ( (op= 'not' | op= '-' ) )? shortcutOperatorExpression ;
    public final Epl_EolParserRules.unaryExpression_return unaryExpression() throws RecognitionException {
        Epl_EolParserRules.unaryExpression_return retval = new Epl_EolParserRules.unaryExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Epl_EolParserRules.shortcutOperatorExpression_return shortcutOperatorExpression117 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;

        try {
            // EolParserRules.g:417:2: ( ( (op= 'not' | op= '-' ) )? shortcutOperatorExpression )
            // EolParserRules.g:417:4: ( (op= 'not' | op= '-' ) )? shortcutOperatorExpression
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            // EolParserRules.g:417:4: ( (op= 'not' | op= '-' ) )?
            int alt51=2;
            int LA51_0 = input.LA(1);

            if ( (LA51_0==153||LA51_0==156) ) {
                alt51=1;
            }
            switch (alt51) {
                case 1 :
                    // EolParserRules.g:417:5: (op= 'not' | op= '-' )
                    {
                    // EolParserRules.g:417:5: (op= 'not' | op= '-' )
                    int alt50=2;
                    int LA50_0 = input.LA(1);

                    if ( (LA50_0==156) ) {
                        alt50=1;
                    }
                    else if ( (LA50_0==153) ) {
                        alt50=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 50, 0, input);

                        throw nvae;
                    }
                    switch (alt50) {
                        case 1 :
                            // EolParserRules.g:417:6: op= 'not'
                            {
                            op=(Token)match(input,156,FOLLOW_156_in_unaryExpression1827); if (state.failed) return retval;
                            if ( state.backtracking==0 ) {
                            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
                            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
                            }

                            }
                            break;
                        case 2 :
                            // EolParserRules.g:417:16: op= '-'
                            {
                            op=(Token)match(input,153,FOLLOW_153_in_unaryExpression1832); if (state.failed) return retval;
                            if ( state.backtracking==0 ) {
                            op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
                            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
                            }

                            }
                            break;

                    }

                    if ( state.backtracking==0 ) {
                      op.setType(OPERATOR);
                    }

                    }
                    break;

            }

            pushFollow(FOLLOW_shortcutOperatorExpression_in_unaryExpression1840);
            shortcutOperatorExpression117=shortcutOperatorExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, shortcutOperatorExpression117.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end unaryExpression

    public static class shortcutOperatorExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start shortcutOperatorExpression
    // EolParserRules.g:420:1: shortcutOperatorExpression : postfixExpression ( (op= '++' ) )? ;
    public final Epl_EolParserRules.shortcutOperatorExpression_return shortcutOperatorExpression() throws RecognitionException {
        Epl_EolParserRules.shortcutOperatorExpression_return retval = new Epl_EolParserRules.shortcutOperatorExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Epl_EolParserRules.postfixExpression_return postfixExpression118 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;

        try {
            // EolParserRules.g:421:2: ( postfixExpression ( (op= '++' ) )? )
            // EolParserRules.g:421:4: postfixExpression ( (op= '++' ) )?
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_postfixExpression_in_shortcutOperatorExpression1852);
            postfixExpression118=postfixExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, postfixExpression118.getTree());
            // EolParserRules.g:421:22: ( (op= '++' ) )?
            int alt52=2;
            int LA52_0 = input.LA(1);

            if ( (LA52_0==157) ) {
                alt52=1;
            }
            switch (alt52) {
                case 1 :
                    // EolParserRules.g:421:23: (op= '++' )
                    {
                    // EolParserRules.g:421:23: (op= '++' )
                    // EolParserRules.g:421:24: op= '++'
                    {
                    op=(Token)match(input,157,FOLLOW_157_in_shortcutOperatorExpression1858); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    op_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(op);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(op_tree, root_0);
                    }

                    }

                    if ( state.backtracking==0 ) {
                      op.setType(OPERATOR);
                    }

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end shortcutOperatorExpression

    public static class postfixExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start postfixExpression
    // EolParserRules.g:424:1: postfixExpression : itemSelectorExpression ( ( POINT | ARROW ) fc= featureCall (is= '[' logicalExpression ']' )* )* ;
    public final Epl_EolParserRules.postfixExpression_return postfixExpression() throws RecognitionException {
        Epl_EolParserRules.postfixExpression_return retval = new Epl_EolParserRules.postfixExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token is=null;
        Token set120=null;
        Token char_literal122=null;
        Epl_EolParserRules.featureCall_return fc = null;

        Epl_EolParserRules.itemSelectorExpression_return itemSelectorExpression119 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression121 = null;


        org.eclipse.epsilon.common.parse.AST is_tree=null;
        org.eclipse.epsilon.common.parse.AST set120_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal122_tree=null;

        try {
            // EolParserRules.g:425:2: ( itemSelectorExpression ( ( POINT | ARROW ) fc= featureCall (is= '[' logicalExpression ']' )* )* )
            // EolParserRules.g:425:4: itemSelectorExpression ( ( POINT | ARROW ) fc= featureCall (is= '[' logicalExpression ']' )* )*
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_itemSelectorExpression_in_postfixExpression1876);
            itemSelectorExpression119=itemSelectorExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, itemSelectorExpression119.getTree());
            // EolParserRules.g:425:27: ( ( POINT | ARROW ) fc= featureCall (is= '[' logicalExpression ']' )* )*
            loop54:
            do {
                int alt54=2;
                int LA54_0 = input.LA(1);

                if ( (LA54_0==POINT||LA54_0==ARROW) ) {
                    alt54=1;
                }


                switch (alt54) {
            	case 1 :
            	    // EolParserRules.g:425:28: ( POINT | ARROW ) fc= featureCall (is= '[' logicalExpression ']' )*
            	    {
            	    set120=(Token)input.LT(1);
            	    set120=(Token)input.LT(1);
            	    if ( input.LA(1)==POINT||input.LA(1)==ARROW ) {
            	        input.consume();
            	        if ( state.backtracking==0 ) root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(set120), root_0);
            	        state.errorRecovery=false;state.failed=false;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        throw mse;
            	    }

            	    pushFollow(FOLLOW_featureCall_in_postfixExpression1888);
            	    fc=featureCall();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, fc.getTree());
            	    if ( state.backtracking==0 ) {
            	      setTokenType(fc,FEATURECALL);
            	    }
            	    // EolParserRules.g:426:35: (is= '[' logicalExpression ']' )*
            	    loop53:
            	    do {
            	        int alt53=2;
            	        int LA53_0 = input.LA(1);

            	        if ( (LA53_0==158) ) {
            	            alt53=1;
            	        }


            	        switch (alt53) {
            	    	case 1 :
            	    	    // EolParserRules.g:426:36: is= '[' logicalExpression ']'
            	    	    {
            	    	    is=(Token)match(input,158,FOLLOW_158_in_postfixExpression1897); if (state.failed) return retval;
            	    	    if ( state.backtracking==0 ) {
            	    	    is_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(is);
            	    	    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(is_tree, root_0);
            	    	    }
            	    	    pushFollow(FOLLOW_logicalExpression_in_postfixExpression1900);
            	    	    logicalExpression121=logicalExpression();

            	    	    state._fsp--;
            	    	    if (state.failed) return retval;
            	    	    if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression121.getTree());
            	    	    char_literal122=(Token)match(input,159,FOLLOW_159_in_postfixExpression1902); if (state.failed) return retval;
            	    	    if ( state.backtracking==0 ) {
            	    	      is.setType(ITEMSELECTOR);
            	    	    }

            	    	    }
            	    	    break;

            	    	default :
            	    	    break loop53;
            	        }
            	    } while (true);


            	    }
            	    break;

            	default :
            	    break loop54;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end postfixExpression

    public static class itemSelectorExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start itemSelectorExpression
    // EolParserRules.g:430:1: itemSelectorExpression : primitiveExpression (is= '[' primitiveExpression ']' )* ;
    public final Epl_EolParserRules.itemSelectorExpression_return itemSelectorExpression() throws RecognitionException {
        Epl_EolParserRules.itemSelectorExpression_return retval = new Epl_EolParserRules.itemSelectorExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token is=null;
        Token char_literal125=null;
        Epl_EolParserRules.primitiveExpression_return primitiveExpression123 = null;

        Epl_EolParserRules.primitiveExpression_return primitiveExpression124 = null;


        org.eclipse.epsilon.common.parse.AST is_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal125_tree=null;

        try {
            // EolParserRules.g:431:2: ( primitiveExpression (is= '[' primitiveExpression ']' )* )
            // EolParserRules.g:431:4: primitiveExpression (is= '[' primitiveExpression ']' )*
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_primitiveExpression_in_itemSelectorExpression1924);
            primitiveExpression123=primitiveExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, primitiveExpression123.getTree());
            // EolParserRules.g:431:24: (is= '[' primitiveExpression ']' )*
            loop55:
            do {
                int alt55=2;
                int LA55_0 = input.LA(1);

                if ( (LA55_0==158) ) {
                    alt55=1;
                }


                switch (alt55) {
            	case 1 :
            	    // EolParserRules.g:431:25: is= '[' primitiveExpression ']'
            	    {
            	    is=(Token)match(input,158,FOLLOW_158_in_itemSelectorExpression1929); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    is_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(is);
            	    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(is_tree, root_0);
            	    }
            	    pushFollow(FOLLOW_primitiveExpression_in_itemSelectorExpression1932);
            	    primitiveExpression124=primitiveExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, primitiveExpression124.getTree());
            	    char_literal125=(Token)match(input,159,FOLLOW_159_in_itemSelectorExpression1934); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	      is.setType(ITEMSELECTOR);
            	    }

            	    }
            	    break;

            	default :
            	    break loop55;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end itemSelectorExpression

    public static class featureCall_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start featureCall
    // EolParserRules.g:435:1: featureCall : ( simpleFeatureCall | complexFeatureCall );
    public final Epl_EolParserRules.featureCall_return featureCall() throws RecognitionException {
        Epl_EolParserRules.featureCall_return retval = new Epl_EolParserRules.featureCall_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.simpleFeatureCall_return simpleFeatureCall126 = null;

        Epl_EolParserRules.complexFeatureCall_return complexFeatureCall127 = null;



        try {
            // EolParserRules.g:436:2: ( simpleFeatureCall | complexFeatureCall )
            int alt56=2;
            alt56 = dfa56.predict(input);
            switch (alt56) {
                case 1 :
                    // EolParserRules.g:436:4: simpleFeatureCall
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_simpleFeatureCall_in_featureCall1952);
                    simpleFeatureCall126=simpleFeatureCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, simpleFeatureCall126.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:436:24: complexFeatureCall
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_complexFeatureCall_in_featureCall1956);
                    complexFeatureCall127=complexFeatureCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, complexFeatureCall127.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end featureCall

    public static class simpleFeatureCall_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start simpleFeatureCall
    // EolParserRules.g:439:1: simpleFeatureCall : n= NAME ( parameterList )? ;
    public final Epl_EolParserRules.simpleFeatureCall_return simpleFeatureCall() throws RecognitionException {
        Epl_EolParserRules.simpleFeatureCall_return retval = new Epl_EolParserRules.simpleFeatureCall_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token n=null;
        Epl_EolParserRules.parameterList_return parameterList128 = null;


        org.eclipse.epsilon.common.parse.AST n_tree=null;

        try {
            // EolParserRules.g:440:2: (n= NAME ( parameterList )? )
            // EolParserRules.g:440:5: n= NAME ( parameterList )?
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            n=(Token)match(input,NAME,FOLLOW_NAME_in_simpleFeatureCall1970); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            n_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(n);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(n_tree, root_0);
            }
            // EolParserRules.g:440:13: ( parameterList )?
            int alt57=2;
            int LA57_0 = input.LA(1);

            if ( (LA57_0==104) ) {
                alt57=1;
            }
            switch (alt57) {
                case 1 :
                    // EolParserRules.g:0:0: parameterList
                    {
                    pushFollow(FOLLOW_parameterList_in_simpleFeatureCall1973);
                    parameterList128=parameterList();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parameterList128.getTree());

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
              n.setType(FEATURECALL);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end simpleFeatureCall

    public static class parameterList_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start parameterList
    // EolParserRules.g:444:1: parameterList : op= '(' ( logicalExpression ( ',' logicalExpression )* )? cp= ')' -> ^( PARAMETERS ( logicalExpression )* ) ;
    public final Epl_EolParserRules.parameterList_return parameterList() throws RecognitionException {
        Epl_EolParserRules.parameterList_return retval = new Epl_EolParserRules.parameterList_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Token cp=null;
        Token char_literal130=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression129 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression131 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;
        org.eclipse.epsilon.common.parse.AST cp_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal130_tree=null;
        RewriteRuleTokenStream stream_104=new RewriteRuleTokenStream(adaptor,"token 104");
        RewriteRuleTokenStream stream_105=new RewriteRuleTokenStream(adaptor,"token 105");
        RewriteRuleTokenStream stream_97=new RewriteRuleTokenStream(adaptor,"token 97");
        RewriteRuleSubtreeStream stream_logicalExpression=new RewriteRuleSubtreeStream(adaptor,"rule logicalExpression");
        try {
            // EolParserRules.g:450:2: (op= '(' ( logicalExpression ( ',' logicalExpression )* )? cp= ')' -> ^( PARAMETERS ( logicalExpression )* ) )
            // EolParserRules.g:450:4: op= '(' ( logicalExpression ( ',' logicalExpression )* )? cp= ')'
            {
            op=(Token)match(input,104,FOLLOW_104_in_parameterList1996); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_104.add(op);

            // EolParserRules.g:450:11: ( logicalExpression ( ',' logicalExpression )* )?
            int alt59=2;
            int LA59_0 = input.LA(1);

            if ( (LA59_0==FLOAT||LA59_0==INT||LA59_0==BOOLEAN||LA59_0==STRING||LA59_0==NAME||LA59_0==104||(LA59_0>=112 && LA59_0<=119)||LA59_0==153||LA59_0==156||(LA59_0>=162 && LA59_0<=164)) ) {
                alt59=1;
            }
            switch (alt59) {
                case 1 :
                    // EolParserRules.g:450:12: logicalExpression ( ',' logicalExpression )*
                    {
                    pushFollow(FOLLOW_logicalExpression_in_parameterList1999);
                    logicalExpression129=logicalExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_logicalExpression.add(logicalExpression129.getTree());
                    // EolParserRules.g:450:30: ( ',' logicalExpression )*
                    loop58:
                    do {
                        int alt58=2;
                        int LA58_0 = input.LA(1);

                        if ( (LA58_0==97) ) {
                            alt58=1;
                        }


                        switch (alt58) {
                    	case 1 :
                    	    // EolParserRules.g:450:31: ',' logicalExpression
                    	    {
                    	    char_literal130=(Token)match(input,97,FOLLOW_97_in_parameterList2002); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_97.add(char_literal130);

                    	    pushFollow(FOLLOW_logicalExpression_in_parameterList2004);
                    	    logicalExpression131=logicalExpression();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_logicalExpression.add(logicalExpression131.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop58;
                        }
                    } while (true);


                    }
                    break;

            }

            cp=(Token)match(input,105,FOLLOW_105_in_parameterList2012); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_105.add(cp);



            // AST REWRITE
            // elements: logicalExpression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
            // 451:3: -> ^( PARAMETERS ( logicalExpression )* )
            {
                // EolParserRules.g:451:6: ^( PARAMETERS ( logicalExpression )* )
                {
                org.eclipse.epsilon.common.parse.AST root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
                root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(PARAMETERS, "PARAMETERS"), root_1);

                // EolParserRules.g:451:19: ( logicalExpression )*
                while ( stream_logicalExpression.hasNext() ) {
                    adaptor.addChild(root_1, stream_logicalExpression.nextTree());

                }
                stream_logicalExpression.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).setImaginary(true);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(op);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(cp);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end parameterList

    public static class complexFeatureCall_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start complexFeatureCall
    // EolParserRules.g:454:1: complexFeatureCall : NAME op= '(' ( lambdaExpression | ( complexParameterExpression ',' ( complexParameterExpression ',' )* lambdaExpressionInBrackets ) ) ( ',' ( logicalExpression | lambdaExpressionInBrackets ) )* cp= ')' ;
    public final Epl_EolParserRules.complexFeatureCall_return complexFeatureCall() throws RecognitionException {
        Epl_EolParserRules.complexFeatureCall_return retval = new Epl_EolParserRules.complexFeatureCall_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token op=null;
        Token cp=null;
        Token NAME132=null;
        Token char_literal135=null;
        Token char_literal137=null;
        Token char_literal139=null;
        Epl_EolParserRules.lambdaExpression_return lambdaExpression133 = null;

        Epl_EolParserRules.complexParameterExpression_return complexParameterExpression134 = null;

        Epl_EolParserRules.complexParameterExpression_return complexParameterExpression136 = null;

        Epl_EolParserRules.lambdaExpressionInBrackets_return lambdaExpressionInBrackets138 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression140 = null;

        Epl_EolParserRules.lambdaExpressionInBrackets_return lambdaExpressionInBrackets141 = null;


        org.eclipse.epsilon.common.parse.AST op_tree=null;
        org.eclipse.epsilon.common.parse.AST cp_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME132_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal135_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal137_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal139_tree=null;

        try {
            // EolParserRules.g:459:2: ( NAME op= '(' ( lambdaExpression | ( complexParameterExpression ',' ( complexParameterExpression ',' )* lambdaExpressionInBrackets ) ) ( ',' ( logicalExpression | lambdaExpressionInBrackets ) )* cp= ')' )
            // EolParserRules.g:459:4: NAME op= '(' ( lambdaExpression | ( complexParameterExpression ',' ( complexParameterExpression ',' )* lambdaExpressionInBrackets ) ) ( ',' ( logicalExpression | lambdaExpressionInBrackets ) )* cp= ')'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            NAME132=(Token)match(input,NAME,FOLLOW_NAME_in_complexFeatureCall2040); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            NAME132_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME132);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(NAME132_tree, root_0);
            }
            op=(Token)match(input,104,FOLLOW_104_in_complexFeatureCall2045); if (state.failed) return retval;
            // EolParserRules.g:459:18: ( lambdaExpression | ( complexParameterExpression ',' ( complexParameterExpression ',' )* lambdaExpressionInBrackets ) )
            int alt61=2;
            alt61 = dfa61.predict(input);
            switch (alt61) {
                case 1 :
                    // EolParserRules.g:459:19: lambdaExpression
                    {
                    pushFollow(FOLLOW_lambdaExpression_in_complexFeatureCall2049);
                    lambdaExpression133=lambdaExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, lambdaExpression133.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:459:38: ( complexParameterExpression ',' ( complexParameterExpression ',' )* lambdaExpressionInBrackets )
                    {
                    // EolParserRules.g:459:38: ( complexParameterExpression ',' ( complexParameterExpression ',' )* lambdaExpressionInBrackets )
                    // EolParserRules.g:459:39: complexParameterExpression ',' ( complexParameterExpression ',' )* lambdaExpressionInBrackets
                    {
                    pushFollow(FOLLOW_complexParameterExpression_in_complexFeatureCall2054);
                    complexParameterExpression134=complexParameterExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, complexParameterExpression134.getTree());
                    char_literal135=(Token)match(input,97,FOLLOW_97_in_complexFeatureCall2056); if (state.failed) return retval;
                    // EolParserRules.g:459:71: ( complexParameterExpression ',' )*
                    loop60:
                    do {
                        int alt60=2;
                        int LA60_0 = input.LA(1);

                        if ( (LA60_0==FLOAT||LA60_0==INT||LA60_0==BOOLEAN||LA60_0==STRING||LA60_0==NAME||LA60_0==112) ) {
                            alt60=1;
                        }


                        switch (alt60) {
                    	case 1 :
                    	    // EolParserRules.g:459:72: complexParameterExpression ','
                    	    {
                    	    pushFollow(FOLLOW_complexParameterExpression_in_complexFeatureCall2060);
                    	    complexParameterExpression136=complexParameterExpression();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) adaptor.addChild(root_0, complexParameterExpression136.getTree());
                    	    char_literal137=(Token)match(input,97,FOLLOW_97_in_complexFeatureCall2062); if (state.failed) return retval;

                    	    }
                    	    break;

                    	default :
                    	    break loop60;
                        }
                    } while (true);

                    pushFollow(FOLLOW_lambdaExpressionInBrackets_in_complexFeatureCall2067);
                    lambdaExpressionInBrackets138=lambdaExpressionInBrackets();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, lambdaExpressionInBrackets138.getTree());

                    }


                    }
                    break;

            }

            // EolParserRules.g:460:3: ( ',' ( logicalExpression | lambdaExpressionInBrackets ) )*
            loop63:
            do {
                int alt63=2;
                int LA63_0 = input.LA(1);

                if ( (LA63_0==97) ) {
                    alt63=1;
                }


                switch (alt63) {
            	case 1 :
            	    // EolParserRules.g:460:4: ',' ( logicalExpression | lambdaExpressionInBrackets )
            	    {
            	    char_literal139=(Token)match(input,97,FOLLOW_97_in_complexFeatureCall2074); if (state.failed) return retval;
            	    // EolParserRules.g:460:9: ( logicalExpression | lambdaExpressionInBrackets )
            	    int alt62=2;
            	    switch ( input.LA(1) ) {
            	    case FLOAT:
            	    case INT:
            	    case BOOLEAN:
            	    case STRING:
            	    case NAME:
            	    case 112:
            	    case 113:
            	    case 114:
            	    case 115:
            	    case 116:
            	    case 117:
            	    case 118:
            	    case 119:
            	    case 153:
            	    case 156:
            	    case 162:
            	    case 163:
            	    case 164:
            	        {
            	        alt62=1;
            	        }
            	        break;
            	    case 104:
            	        {
            	        switch ( input.LA(2) ) {
            	        case NAME:
            	            {
            	            int LA62_4 = input.LA(3);

            	            if ( (LA62_4==POINT||LA62_4==ARROW||LA62_4==101||(LA62_4>=104 && LA62_4<=105)||(LA62_4>=109 && LA62_4<=111)||(LA62_4>=120 && LA62_4<=121)||(LA62_4>=144 && LA62_4<=155)||(LA62_4>=157 && LA62_4<=158)) ) {
            	                alt62=1;
            	            }
            	            else if ( (LA62_4==97||LA62_4==106||(LA62_4>=160 && LA62_4<=161)) ) {
            	                alt62=2;
            	            }
            	            else {
            	                if (state.backtracking>0) {state.failed=true; return retval;}
            	                NoViableAltException nvae =
            	                    new NoViableAltException("", 62, 4, input);

            	                throw nvae;
            	            }
            	            }
            	            break;
            	        case 160:
            	        case 161:
            	            {
            	            alt62=2;
            	            }
            	            break;
            	        case FLOAT:
            	        case INT:
            	        case BOOLEAN:
            	        case STRING:
            	        case 104:
            	        case 112:
            	        case 113:
            	        case 114:
            	        case 115:
            	        case 116:
            	        case 117:
            	        case 118:
            	        case 119:
            	        case 153:
            	        case 156:
            	        case 162:
            	        case 163:
            	        case 164:
            	            {
            	            alt62=1;
            	            }
            	            break;
            	        default:
            	            if (state.backtracking>0) {state.failed=true; return retval;}
            	            NoViableAltException nvae =
            	                new NoViableAltException("", 62, 2, input);

            	            throw nvae;
            	        }

            	        }
            	        break;
            	    case 158:
            	        {
            	        alt62=2;
            	        }
            	        break;
            	    default:
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 62, 0, input);

            	        throw nvae;
            	    }

            	    switch (alt62) {
            	        case 1 :
            	            // EolParserRules.g:460:10: logicalExpression
            	            {
            	            pushFollow(FOLLOW_logicalExpression_in_complexFeatureCall2078);
            	            logicalExpression140=logicalExpression();

            	            state._fsp--;
            	            if (state.failed) return retval;
            	            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression140.getTree());

            	            }
            	            break;
            	        case 2 :
            	            // EolParserRules.g:460:30: lambdaExpressionInBrackets
            	            {
            	            pushFollow(FOLLOW_lambdaExpressionInBrackets_in_complexFeatureCall2082);
            	            lambdaExpressionInBrackets141=lambdaExpressionInBrackets();

            	            state._fsp--;
            	            if (state.failed) return retval;
            	            if ( state.backtracking==0 ) adaptor.addChild(root_0, lambdaExpressionInBrackets141.getTree());

            	            }
            	            break;

            	    }


            	    }
            	    break;

            	default :
            	    break loop63;
                }
            } while (true);

            cp=(Token)match(input,105,FOLLOW_105_in_complexFeatureCall2089); if (state.failed) return retval;

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(op);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(cp);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end complexFeatureCall

    public static class complexParameterExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start complexParameterExpression
    // EolParserRules.g:463:1: complexParameterExpression : ( NAME | literal | nativeType | pathName );
    public final Epl_EolParserRules.complexParameterExpression_return complexParameterExpression() throws RecognitionException {
        Epl_EolParserRules.complexParameterExpression_return retval = new Epl_EolParserRules.complexParameterExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token NAME142=null;
        Epl_EolParserRules.literal_return literal143 = null;

        Epl_EolParserRules.nativeType_return nativeType144 = null;

        Epl_EolParserRules.pathName_return pathName145 = null;


        org.eclipse.epsilon.common.parse.AST NAME142_tree=null;

        try {
            // EolParserRules.g:464:2: ( NAME | literal | nativeType | pathName )
            int alt64=4;
            switch ( input.LA(1) ) {
            case NAME:
                {
                int LA64_1 = input.LA(2);

                if ( (synpred94_EolParserRules()) ) {
                    alt64=1;
                }
                else if ( (true) ) {
                    alt64=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 64, 1, input);

                    throw nvae;
                }
                }
                break;
            case FLOAT:
            case INT:
            case BOOLEAN:
            case STRING:
                {
                alt64=2;
                }
                break;
            case 112:
                {
                alt64=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 64, 0, input);

                throw nvae;
            }

            switch (alt64) {
                case 1 :
                    // EolParserRules.g:464:4: NAME
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    NAME142=(Token)match(input,NAME,FOLLOW_NAME_in_complexParameterExpression2101); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    NAME142_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME142);
                    adaptor.addChild(root_0, NAME142_tree);
                    }

                    }
                    break;
                case 2 :
                    // EolParserRules.g:464:11: literal
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_complexParameterExpression2105);
                    literal143=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal143.getTree());

                    }
                    break;
                case 3 :
                    // EolParserRules.g:464:21: nativeType
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_nativeType_in_complexParameterExpression2109);
                    nativeType144=nativeType();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, nativeType144.getTree());

                    }
                    break;
                case 4 :
                    // EolParserRules.g:464:34: pathName
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_pathName_in_complexParameterExpression2113);
                    pathName145=pathName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, pathName145.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end complexParameterExpression

    public static class lambdaExpressionInBrackets_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start lambdaExpressionInBrackets
    // EolParserRules.g:467:1: lambdaExpressionInBrackets : ( (lop= '[' lambdaExpression lcp= ']' ) | (lop= '(' lambdaExpression lcp= ')' ) );
    public final Epl_EolParserRules.lambdaExpressionInBrackets_return lambdaExpressionInBrackets() throws RecognitionException {
        Epl_EolParserRules.lambdaExpressionInBrackets_return retval = new Epl_EolParserRules.lambdaExpressionInBrackets_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token lop=null;
        Token lcp=null;
        Epl_EolParserRules.lambdaExpression_return lambdaExpression146 = null;

        Epl_EolParserRules.lambdaExpression_return lambdaExpression147 = null;


        org.eclipse.epsilon.common.parse.AST lop_tree=null;
        org.eclipse.epsilon.common.parse.AST lcp_tree=null;

        try {
            // EolParserRules.g:473:2: ( (lop= '[' lambdaExpression lcp= ']' ) | (lop= '(' lambdaExpression lcp= ')' ) )
            int alt65=2;
            int LA65_0 = input.LA(1);

            if ( (LA65_0==158) ) {
                alt65=1;
            }
            else if ( (LA65_0==104) ) {
                alt65=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 65, 0, input);

                throw nvae;
            }
            switch (alt65) {
                case 1 :
                    // EolParserRules.g:473:4: (lop= '[' lambdaExpression lcp= ']' )
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    // EolParserRules.g:473:4: (lop= '[' lambdaExpression lcp= ']' )
                    // EolParserRules.g:473:5: lop= '[' lambdaExpression lcp= ']'
                    {
                    lop=(Token)match(input,158,FOLLOW_158_in_lambdaExpressionInBrackets2133); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    lop_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(lop);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(lop_tree, root_0);
                    }
                    pushFollow(FOLLOW_lambdaExpression_in_lambdaExpressionInBrackets2136);
                    lambdaExpression146=lambdaExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, lambdaExpression146.getTree());
                    lcp=(Token)match(input,159,FOLLOW_159_in_lambdaExpressionInBrackets2140); if (state.failed) return retval;

                    }


                    }
                    break;
                case 2 :
                    // EolParserRules.g:474:3: (lop= '(' lambdaExpression lcp= ')' )
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    // EolParserRules.g:474:3: (lop= '(' lambdaExpression lcp= ')' )
                    // EolParserRules.g:474:4: lop= '(' lambdaExpression lcp= ')'
                    {
                    lop=(Token)match(input,104,FOLLOW_104_in_lambdaExpressionInBrackets2151); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    lop_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(lop);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(lop_tree, root_0);
                    }
                    pushFollow(FOLLOW_lambdaExpression_in_lambdaExpressionInBrackets2154);
                    lambdaExpression147=lambdaExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, lambdaExpression147.getTree());
                    lcp=(Token)match(input,105,FOLLOW_105_in_lambdaExpressionInBrackets2158); if (state.failed) return retval;

                    }


                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		lop.setType(LAMBDAEXPR);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(lop);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(lcp);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end lambdaExpressionInBrackets

    public static class lambdaExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start lambdaExpression
    // EolParserRules.g:477:1: lambdaExpression : ( formalParameterList )? (lt= '|' | lt= '=>' ) logicalExpression ;
    public final Epl_EolParserRules.lambdaExpression_return lambdaExpression() throws RecognitionException {
        Epl_EolParserRules.lambdaExpression_return retval = new Epl_EolParserRules.lambdaExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token lt=null;
        Epl_EolParserRules.formalParameterList_return formalParameterList148 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression149 = null;


        org.eclipse.epsilon.common.parse.AST lt_tree=null;

        try {
            // EolParserRules.g:481:2: ( ( formalParameterList )? (lt= '|' | lt= '=>' ) logicalExpression )
            // EolParserRules.g:481:4: ( formalParameterList )? (lt= '|' | lt= '=>' ) logicalExpression
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            // EolParserRules.g:481:4: ( formalParameterList )?
            int alt66=2;
            int LA66_0 = input.LA(1);

            if ( (LA66_0==NAME) ) {
                alt66=1;
            }
            switch (alt66) {
                case 1 :
                    // EolParserRules.g:0:0: formalParameterList
                    {
                    pushFollow(FOLLOW_formalParameterList_in_lambdaExpression2177);
                    formalParameterList148=formalParameterList();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, formalParameterList148.getTree());

                    }
                    break;

            }

            // EolParserRules.g:481:25: (lt= '|' | lt= '=>' )
            int alt67=2;
            int LA67_0 = input.LA(1);

            if ( (LA67_0==160) ) {
                alt67=1;
            }
            else if ( (LA67_0==161) ) {
                alt67=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 67, 0, input);

                throw nvae;
            }
            switch (alt67) {
                case 1 :
                    // EolParserRules.g:481:26: lt= '|'
                    {
                    lt=(Token)match(input,160,FOLLOW_160_in_lambdaExpression2183); if (state.failed) return retval;

                    }
                    break;
                case 2 :
                    // EolParserRules.g:481:36: lt= '=>'
                    {
                    lt=(Token)match(input,161,FOLLOW_161_in_lambdaExpression2190); if (state.failed) return retval;

                    }
                    break;

            }

            pushFollow(FOLLOW_logicalExpression_in_lambdaExpression2194);
            logicalExpression149=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression149.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(lt);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end lambdaExpression

    public static class newExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start newExpression
    // EolParserRules.g:484:1: newExpression : n= 'new' tn= typeName ( parameterList )? ;
    public final Epl_EolParserRules.newExpression_return newExpression() throws RecognitionException {
        Epl_EolParserRules.newExpression_return retval = new Epl_EolParserRules.newExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token n=null;
        Epl_EolParserRules.typeName_return tn = null;

        Epl_EolParserRules.parameterList_return parameterList150 = null;


        org.eclipse.epsilon.common.parse.AST n_tree=null;

        try {
            // EolParserRules.g:485:2: (n= 'new' tn= typeName ( parameterList )? )
            // EolParserRules.g:485:4: n= 'new' tn= typeName ( parameterList )?
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            n=(Token)match(input,162,FOLLOW_162_in_newExpression2207); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            n_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(n);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(n_tree, root_0);
            }
            pushFollow(FOLLOW_typeName_in_newExpression2212);
            tn=typeName();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, tn.getTree());
            if ( state.backtracking==0 ) {
              setTokenType(tn,TYPE);
            }
            // EolParserRules.g:485:50: ( parameterList )?
            int alt68=2;
            int LA68_0 = input.LA(1);

            if ( (LA68_0==104) ) {
                alt68=1;
            }
            switch (alt68) {
                case 1 :
                    // EolParserRules.g:0:0: parameterList
                    {
                    pushFollow(FOLLOW_parameterList_in_newExpression2216);
                    parameterList150=parameterList();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parameterList150.getTree());

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
              n.setType(NEW);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end newExpression

    public static class variableDeclarationExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start variableDeclarationExpression
    // EolParserRules.g:489:1: variableDeclarationExpression : (v= 'var' | v= 'ext' ) NAME ( ':' (n= 'new' )? t= typeName ( parameterList )? )? ;
    public final Epl_EolParserRules.variableDeclarationExpression_return variableDeclarationExpression() throws RecognitionException {
        Epl_EolParserRules.variableDeclarationExpression_return retval = new Epl_EolParserRules.variableDeclarationExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token v=null;
        Token n=null;
        Token NAME151=null;
        Token char_literal152=null;
        Epl_EolParserRules.typeName_return t = null;

        Epl_EolParserRules.parameterList_return parameterList153 = null;


        org.eclipse.epsilon.common.parse.AST v_tree=null;
        org.eclipse.epsilon.common.parse.AST n_tree=null;
        org.eclipse.epsilon.common.parse.AST NAME151_tree=null;
        org.eclipse.epsilon.common.parse.AST char_literal152_tree=null;

        try {
            // EolParserRules.g:495:2: ( (v= 'var' | v= 'ext' ) NAME ( ':' (n= 'new' )? t= typeName ( parameterList )? )? )
            // EolParserRules.g:495:4: (v= 'var' | v= 'ext' ) NAME ( ':' (n= 'new' )? t= typeName ( parameterList )? )?
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            // EolParserRules.g:495:4: (v= 'var' | v= 'ext' )
            int alt69=2;
            int LA69_0 = input.LA(1);

            if ( (LA69_0==163) ) {
                alt69=1;
            }
            else if ( (LA69_0==164) ) {
                alt69=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 69, 0, input);

                throw nvae;
            }
            switch (alt69) {
                case 1 :
                    // EolParserRules.g:495:5: v= 'var'
                    {
                    v=(Token)match(input,163,FOLLOW_163_in_variableDeclarationExpression2240); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    v_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(v);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(v_tree, root_0);
                    }

                    }
                    break;
                case 2 :
                    // EolParserRules.g:495:14: v= 'ext'
                    {
                    v=(Token)match(input,164,FOLLOW_164_in_variableDeclarationExpression2245); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    v_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(v);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(v_tree, root_0);
                    }

                    }
                    break;

            }

            NAME151=(Token)match(input,NAME,FOLLOW_NAME_in_variableDeclarationExpression2249); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            NAME151_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(NAME151);
            adaptor.addChild(root_0, NAME151_tree);
            }
            // EolParserRules.g:495:29: ( ':' (n= 'new' )? t= typeName ( parameterList )? )?
            int alt72=2;
            alt72 = dfa72.predict(input);
            switch (alt72) {
                case 1 :
                    // EolParserRules.g:495:30: ':' (n= 'new' )? t= typeName ( parameterList )?
                    {
                    char_literal152=(Token)match(input,106,FOLLOW_106_in_variableDeclarationExpression2252); if (state.failed) return retval;
                    // EolParserRules.g:495:36: (n= 'new' )?
                    int alt70=2;
                    int LA70_0 = input.LA(1);

                    if ( (LA70_0==162) ) {
                        alt70=1;
                    }
                    switch (alt70) {
                        case 1 :
                            // EolParserRules.g:0:0: n= 'new'
                            {
                            n=(Token)match(input,162,FOLLOW_162_in_variableDeclarationExpression2257); if (state.failed) return retval;

                            }
                            break;

                    }

                    pushFollow(FOLLOW_typeName_in_variableDeclarationExpression2263);
                    t=typeName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, t.getTree());
                    if ( state.backtracking==0 ) {
                      setTokenType(t, TYPE);
                    }
                    // EolParserRules.g:495:81: ( parameterList )?
                    int alt71=2;
                    int LA71_0 = input.LA(1);

                    if ( (LA71_0==104) ) {
                        alt71=1;
                    }
                    switch (alt71) {
                        case 1 :
                            // EolParserRules.g:0:0: parameterList
                            {
                            pushFollow(FOLLOW_parameterList_in_variableDeclarationExpression2267);
                            parameterList153=parameterList();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) adaptor.addChild(root_0, parameterList153.getTree());

                            }
                            break;

                    }


                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		String txt = n != null ? "new" : v.getText();
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getToken().setText(txt);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getToken().setType(VAR);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end variableDeclarationExpression

    public static class literalSequentialCollection_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start literalSequentialCollection
    // EolParserRules.g:498:1: literalSequentialCollection : (l= 'Collection' | l= 'Sequence' | l= 'List' | l= 'Bag' | l= 'Set' | l= 'OrderedSet' ) ob= '{' ( expressionListOrRange )? cb= '}' ;
    public final Epl_EolParserRules.literalSequentialCollection_return literalSequentialCollection() throws RecognitionException {
        Epl_EolParserRules.literalSequentialCollection_return retval = new Epl_EolParserRules.literalSequentialCollection_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token l=null;
        Token ob=null;
        Token cb=null;
        Epl_EolParserRules.expressionListOrRange_return expressionListOrRange154 = null;


        org.eclipse.epsilon.common.parse.AST l_tree=null;
        org.eclipse.epsilon.common.parse.AST ob_tree=null;
        org.eclipse.epsilon.common.parse.AST cb_tree=null;

        try {
            // EolParserRules.g:503:2: ( (l= 'Collection' | l= 'Sequence' | l= 'List' | l= 'Bag' | l= 'Set' | l= 'OrderedSet' ) ob= '{' ( expressionListOrRange )? cb= '}' )
            // EolParserRules.g:503:4: (l= 'Collection' | l= 'Sequence' | l= 'List' | l= 'Bag' | l= 'Set' | l= 'OrderedSet' ) ob= '{' ( expressionListOrRange )? cb= '}'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            // EolParserRules.g:503:4: (l= 'Collection' | l= 'Sequence' | l= 'List' | l= 'Bag' | l= 'Set' | l= 'OrderedSet' )
            int alt73=6;
            switch ( input.LA(1) ) {
            case 113:
                {
                alt73=1;
                }
                break;
            case 114:
                {
                alt73=2;
                }
                break;
            case 115:
                {
                alt73=3;
                }
                break;
            case 116:
                {
                alt73=4;
                }
                break;
            case 117:
                {
                alt73=5;
                }
                break;
            case 118:
                {
                alt73=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 73, 0, input);

                throw nvae;
            }

            switch (alt73) {
                case 1 :
                    // EolParserRules.g:503:5: l= 'Collection'
                    {
                    l=(Token)match(input,113,FOLLOW_113_in_literalSequentialCollection2291); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    l_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(l);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(l_tree, root_0);
                    }

                    }
                    break;
                case 2 :
                    // EolParserRules.g:503:21: l= 'Sequence'
                    {
                    l=(Token)match(input,114,FOLLOW_114_in_literalSequentialCollection2296); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    l_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(l);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(l_tree, root_0);
                    }

                    }
                    break;
                case 3 :
                    // EolParserRules.g:503:35: l= 'List'
                    {
                    l=(Token)match(input,115,FOLLOW_115_in_literalSequentialCollection2301); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    l_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(l);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(l_tree, root_0);
                    }

                    }
                    break;
                case 4 :
                    // EolParserRules.g:503:45: l= 'Bag'
                    {
                    l=(Token)match(input,116,FOLLOW_116_in_literalSequentialCollection2306); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    l_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(l);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(l_tree, root_0);
                    }

                    }
                    break;
                case 5 :
                    // EolParserRules.g:503:54: l= 'Set'
                    {
                    l=(Token)match(input,117,FOLLOW_117_in_literalSequentialCollection2311); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    l_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(l);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(l_tree, root_0);
                    }

                    }
                    break;
                case 6 :
                    // EolParserRules.g:503:63: l= 'OrderedSet'
                    {
                    l=(Token)match(input,118,FOLLOW_118_in_literalSequentialCollection2316); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    l_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(l);
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(l_tree, root_0);
                    }

                    }
                    break;

            }

            ob=(Token)match(input,99,FOLLOW_99_in_literalSequentialCollection2322); if (state.failed) return retval;
            // EolParserRules.g:503:89: ( expressionListOrRange )?
            int alt74=2;
            int LA74_0 = input.LA(1);

            if ( (LA74_0==FLOAT||LA74_0==INT||LA74_0==BOOLEAN||LA74_0==STRING||LA74_0==NAME||LA74_0==104||(LA74_0>=112 && LA74_0<=119)||LA74_0==153||LA74_0==156||(LA74_0>=162 && LA74_0<=164)) ) {
                alt74=1;
            }
            switch (alt74) {
                case 1 :
                    // EolParserRules.g:0:0: expressionListOrRange
                    {
                    pushFollow(FOLLOW_expressionListOrRange_in_literalSequentialCollection2326);
                    expressionListOrRange154=expressionListOrRange();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expressionListOrRange154.getTree());

                    }
                    break;

            }

            cb=(Token)match(input,100,FOLLOW_100_in_literalSequentialCollection2331); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              l.setType(COLLECTION);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(ob);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(cb);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end literalSequentialCollection

    public static class expressionRange_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start expressionRange
    // EolParserRules.g:507:1: expressionRange : logicalExpression exp= '..' logicalExpression ;
    public final Epl_EolParserRules.expressionRange_return expressionRange() throws RecognitionException {
        Epl_EolParserRules.expressionRange_return retval = new Epl_EolParserRules.expressionRange_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token exp=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression155 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression156 = null;


        org.eclipse.epsilon.common.parse.AST exp_tree=null;

        try {
            // EolParserRules.g:508:2: ( logicalExpression exp= '..' logicalExpression )
            // EolParserRules.g:508:4: logicalExpression exp= '..' logicalExpression
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_logicalExpression_in_expressionRange2346);
            logicalExpression155=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression155.getTree());
            exp=(Token)match(input,POINT_POINT,FOLLOW_POINT_POINT_in_expressionRange2350); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            exp_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(exp);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(exp_tree, root_0);
            }
            pushFollow(FOLLOW_logicalExpression_in_expressionRange2353);
            logicalExpression156=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression156.getTree());
            if ( state.backtracking==0 ) {
              exp.setType(EXPRRANGE);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end expressionRange

    public static class expressionList_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start expressionList
    // EolParserRules.g:512:1: expressionList : logicalExpression ( ',' logicalExpression )* -> ^( EXPRLIST ( logicalExpression )+ ) ;
    public final Epl_EolParserRules.expressionList_return expressionList() throws RecognitionException {
        Epl_EolParserRules.expressionList_return retval = new Epl_EolParserRules.expressionList_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token char_literal158=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression157 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression159 = null;


        org.eclipse.epsilon.common.parse.AST char_literal158_tree=null;
        RewriteRuleTokenStream stream_97=new RewriteRuleTokenStream(adaptor,"token 97");
        RewriteRuleSubtreeStream stream_logicalExpression=new RewriteRuleSubtreeStream(adaptor,"rule logicalExpression");
        try {
            // EolParserRules.g:516:2: ( logicalExpression ( ',' logicalExpression )* -> ^( EXPRLIST ( logicalExpression )+ ) )
            // EolParserRules.g:516:4: logicalExpression ( ',' logicalExpression )*
            {
            pushFollow(FOLLOW_logicalExpression_in_expressionList2374);
            logicalExpression157=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_logicalExpression.add(logicalExpression157.getTree());
            // EolParserRules.g:516:22: ( ',' logicalExpression )*
            loop75:
            do {
                int alt75=2;
                int LA75_0 = input.LA(1);

                if ( (LA75_0==97) ) {
                    alt75=1;
                }


                switch (alt75) {
            	case 1 :
            	    // EolParserRules.g:516:23: ',' logicalExpression
            	    {
            	    char_literal158=(Token)match(input,97,FOLLOW_97_in_expressionList2377); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_97.add(char_literal158);

            	    pushFollow(FOLLOW_logicalExpression_in_expressionList2379);
            	    logicalExpression159=logicalExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_logicalExpression.add(logicalExpression159.getTree());

            	    }
            	    break;

            	default :
            	    break loop75;
                }
            } while (true);



            // AST REWRITE
            // elements: logicalExpression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
            // 517:2: -> ^( EXPRLIST ( logicalExpression )+ )
            {
                // EolParserRules.g:517:5: ^( EXPRLIST ( logicalExpression )+ )
                {
                org.eclipse.epsilon.common.parse.AST root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
                root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(EXPRLIST, "EXPRLIST"), root_1);

                if ( !(stream_logicalExpression.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_logicalExpression.hasNext() ) {
                    adaptor.addChild(root_1, stream_logicalExpression.nextTree());

                }
                stream_logicalExpression.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).setImaginary(true);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end expressionList

    public static class expressionListOrRange_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start expressionListOrRange
    // EolParserRules.g:520:1: expressionListOrRange : ( expressionRange | expressionList );
    public final Epl_EolParserRules.expressionListOrRange_return expressionListOrRange() throws RecognitionException {
        Epl_EolParserRules.expressionListOrRange_return retval = new Epl_EolParserRules.expressionListOrRange_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.expressionRange_return expressionRange160 = null;

        Epl_EolParserRules.expressionList_return expressionList161 = null;



        try {
            // EolParserRules.g:521:2: ( expressionRange | expressionList )
            int alt76=2;
            alt76 = dfa76.predict(input);
            switch (alt76) {
                case 1 :
                    // EolParserRules.g:521:4: expressionRange
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_expressionRange_in_expressionListOrRange2403);
                    expressionRange160=expressionRange();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expressionRange160.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:521:22: expressionList
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_expressionList_in_expressionListOrRange2407);
                    expressionList161=expressionList();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expressionList161.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end expressionListOrRange

    public static class literalMapCollection_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start literalMapCollection
    // EolParserRules.g:524:1: literalMapCollection : m= 'Map' ob= '{' ( keyvalExpressionList )? cb= '}' ;
    public final Epl_EolParserRules.literalMapCollection_return literalMapCollection() throws RecognitionException {
        Epl_EolParserRules.literalMapCollection_return retval = new Epl_EolParserRules.literalMapCollection_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token m=null;
        Token ob=null;
        Token cb=null;
        Epl_EolParserRules.keyvalExpressionList_return keyvalExpressionList162 = null;


        org.eclipse.epsilon.common.parse.AST m_tree=null;
        org.eclipse.epsilon.common.parse.AST ob_tree=null;
        org.eclipse.epsilon.common.parse.AST cb_tree=null;

        try {
            // EolParserRules.g:529:2: (m= 'Map' ob= '{' ( keyvalExpressionList )? cb= '}' )
            // EolParserRules.g:529:4: m= 'Map' ob= '{' ( keyvalExpressionList )? cb= '}'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            m=(Token)match(input,119,FOLLOW_119_in_literalMapCollection2426); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            m_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(m);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(m_tree, root_0);
            }
            ob=(Token)match(input,99,FOLLOW_99_in_literalMapCollection2431); if (state.failed) return retval;
            // EolParserRules.g:529:21: ( keyvalExpressionList )?
            int alt77=2;
            int LA77_0 = input.LA(1);

            if ( (LA77_0==FLOAT||LA77_0==INT||LA77_0==BOOLEAN||LA77_0==STRING||LA77_0==NAME||LA77_0==104||(LA77_0>=112 && LA77_0<=119)||LA77_0==153||LA77_0==156||(LA77_0>=162 && LA77_0<=164)) ) {
                alt77=1;
            }
            switch (alt77) {
                case 1 :
                    // EolParserRules.g:0:0: keyvalExpressionList
                    {
                    pushFollow(FOLLOW_keyvalExpressionList_in_literalMapCollection2434);
                    keyvalExpressionList162=keyvalExpressionList();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, keyvalExpressionList162.getTree());

                    }
                    break;

            }

            cb=(Token)match(input,100,FOLLOW_100_in_literalMapCollection2439); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              m.setType(MAP);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(ob);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(cb);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end literalMapCollection

    public static class keyvalExpressionList_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start keyvalExpressionList
    // EolParserRules.g:533:1: keyvalExpressionList : keyvalExpression ( ',' keyvalExpression )* -> ^( KEYVALLIST ( keyvalExpression )+ ) ;
    public final Epl_EolParserRules.keyvalExpressionList_return keyvalExpressionList() throws RecognitionException {
        Epl_EolParserRules.keyvalExpressionList_return retval = new Epl_EolParserRules.keyvalExpressionList_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token char_literal164=null;
        Epl_EolParserRules.keyvalExpression_return keyvalExpression163 = null;

        Epl_EolParserRules.keyvalExpression_return keyvalExpression165 = null;


        org.eclipse.epsilon.common.parse.AST char_literal164_tree=null;
        RewriteRuleTokenStream stream_97=new RewriteRuleTokenStream(adaptor,"token 97");
        RewriteRuleSubtreeStream stream_keyvalExpression=new RewriteRuleSubtreeStream(adaptor,"rule keyvalExpression");
        try {
            // EolParserRules.g:537:2: ( keyvalExpression ( ',' keyvalExpression )* -> ^( KEYVALLIST ( keyvalExpression )+ ) )
            // EolParserRules.g:537:4: keyvalExpression ( ',' keyvalExpression )*
            {
            pushFollow(FOLLOW_keyvalExpression_in_keyvalExpressionList2460);
            keyvalExpression163=keyvalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_keyvalExpression.add(keyvalExpression163.getTree());
            // EolParserRules.g:537:21: ( ',' keyvalExpression )*
            loop78:
            do {
                int alt78=2;
                int LA78_0 = input.LA(1);

                if ( (LA78_0==97) ) {
                    alt78=1;
                }


                switch (alt78) {
            	case 1 :
            	    // EolParserRules.g:537:22: ',' keyvalExpression
            	    {
            	    char_literal164=(Token)match(input,97,FOLLOW_97_in_keyvalExpressionList2463); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_97.add(char_literal164);

            	    pushFollow(FOLLOW_keyvalExpression_in_keyvalExpressionList2465);
            	    keyvalExpression165=keyvalExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_keyvalExpression.add(keyvalExpression165.getTree());

            	    }
            	    break;

            	default :
            	    break loop78;
                }
            } while (true);



            // AST REWRITE
            // elements: keyvalExpression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
            // 538:2: -> ^( KEYVALLIST ( keyvalExpression )+ )
            {
                // EolParserRules.g:538:5: ^( KEYVALLIST ( keyvalExpression )+ )
                {
                org.eclipse.epsilon.common.parse.AST root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();
                root_1 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot((org.eclipse.epsilon.common.parse.AST)adaptor.create(KEYVALLIST, "KEYVALLIST"), root_1);

                if ( !(stream_keyvalExpression.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_keyvalExpression.hasNext() ) {
                    adaptor.addChild(root_1, stream_keyvalExpression.nextTree());

                }
                stream_keyvalExpression.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).setImaginary(true);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end keyvalExpressionList

    public static class keyvalExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start keyvalExpression
    // EolParserRules.g:541:1: keyvalExpression : additiveExpression eq= '=' logicalExpression ;
    public final Epl_EolParserRules.keyvalExpression_return keyvalExpression() throws RecognitionException {
        Epl_EolParserRules.keyvalExpression_return retval = new Epl_EolParserRules.keyvalExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token eq=null;
        Epl_EolParserRules.additiveExpression_return additiveExpression166 = null;

        Epl_EolParserRules.logicalExpression_return logicalExpression167 = null;


        org.eclipse.epsilon.common.parse.AST eq_tree=null;

        try {
            // EolParserRules.g:543:2: ( additiveExpression eq= '=' logicalExpression )
            // EolParserRules.g:543:4: additiveExpression eq= '=' logicalExpression
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            pushFollow(FOLLOW_additiveExpression_in_keyvalExpression2490);
            additiveExpression166=additiveExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, additiveExpression166.getTree());
            eq=(Token)match(input,101,FOLLOW_101_in_keyvalExpression2494); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            eq_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(eq);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(eq_tree, root_0);
            }
            pushFollow(FOLLOW_logicalExpression_in_keyvalExpression2497);
            logicalExpression167=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression167.getTree());
            if ( state.backtracking==0 ) {
              eq.setType(KEYVAL);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end keyvalExpression

    public static class primitiveExpression_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start primitiveExpression
    // EolParserRules.g:546:1: primitiveExpression : ( literalSequentialCollection | literalMapCollection | literal | featureCall | pathName | nativeType | collectionType | logicalExpressionInBrackets | newExpression | variableDeclarationExpression );
    public final Epl_EolParserRules.primitiveExpression_return primitiveExpression() throws RecognitionException {
        Epl_EolParserRules.primitiveExpression_return retval = new Epl_EolParserRules.primitiveExpression_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Epl_EolParserRules.literalSequentialCollection_return literalSequentialCollection168 = null;

        Epl_EolParserRules.literalMapCollection_return literalMapCollection169 = null;

        Epl_EolParserRules.literal_return literal170 = null;

        Epl_EolParserRules.featureCall_return featureCall171 = null;

        Epl_EolParserRules.pathName_return pathName172 = null;

        Epl_EolParserRules.nativeType_return nativeType173 = null;

        Epl_EolParserRules.collectionType_return collectionType174 = null;

        Epl_EolParserRules.logicalExpressionInBrackets_return logicalExpressionInBrackets175 = null;

        Epl_EolParserRules.newExpression_return newExpression176 = null;

        Epl_EolParserRules.variableDeclarationExpression_return variableDeclarationExpression177 = null;



        try {
            // EolParserRules.g:547:2: ( literalSequentialCollection | literalMapCollection | literal | featureCall | pathName | nativeType | collectionType | logicalExpressionInBrackets | newExpression | variableDeclarationExpression )
            int alt79=10;
            alt79 = dfa79.predict(input);
            switch (alt79) {
                case 1 :
                    // EolParserRules.g:547:4: literalSequentialCollection
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_literalSequentialCollection_in_primitiveExpression2511);
                    literalSequentialCollection168=literalSequentialCollection();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literalSequentialCollection168.getTree());

                    }
                    break;
                case 2 :
                    // EolParserRules.g:547:34: literalMapCollection
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_literalMapCollection_in_primitiveExpression2515);
                    literalMapCollection169=literalMapCollection();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literalMapCollection169.getTree());

                    }
                    break;
                case 3 :
                    // EolParserRules.g:547:57: literal
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_primitiveExpression2519);
                    literal170=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal170.getTree());

                    }
                    break;
                case 4 :
                    // EolParserRules.g:547:67: featureCall
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_featureCall_in_primitiveExpression2523);
                    featureCall171=featureCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, featureCall171.getTree());

                    }
                    break;
                case 5 :
                    // EolParserRules.g:547:81: pathName
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_pathName_in_primitiveExpression2527);
                    pathName172=pathName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, pathName172.getTree());

                    }
                    break;
                case 6 :
                    // EolParserRules.g:547:92: nativeType
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_nativeType_in_primitiveExpression2531);
                    nativeType173=nativeType();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, nativeType173.getTree());

                    }
                    break;
                case 7 :
                    // EolParserRules.g:548:5: collectionType
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_collectionType_in_primitiveExpression2537);
                    collectionType174=collectionType();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, collectionType174.getTree());

                    }
                    break;
                case 8 :
                    // EolParserRules.g:548:22: logicalExpressionInBrackets
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_logicalExpressionInBrackets_in_primitiveExpression2541);
                    logicalExpressionInBrackets175=logicalExpressionInBrackets();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpressionInBrackets175.getTree());

                    }
                    break;
                case 9 :
                    // EolParserRules.g:549:5: newExpression
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_newExpression_in_primitiveExpression2547);
                    newExpression176=newExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, newExpression176.getTree());

                    }
                    break;
                case 10 :
                    // EolParserRules.g:549:21: variableDeclarationExpression
                    {
                    root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

                    pushFollow(FOLLOW_variableDeclarationExpression_in_primitiveExpression2551);
                    variableDeclarationExpression177=variableDeclarationExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, variableDeclarationExpression177.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end primitiveExpression

    public static class logicalExpressionInBrackets_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start logicalExpressionInBrackets
    // EolParserRules.g:552:1: logicalExpressionInBrackets : ob= '(' logicalExpression cb= ')' ;
    public final Epl_EolParserRules.logicalExpressionInBrackets_return logicalExpressionInBrackets() throws RecognitionException {
        Epl_EolParserRules.logicalExpressionInBrackets_return retval = new Epl_EolParserRules.logicalExpressionInBrackets_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token ob=null;
        Token cb=null;
        Epl_EolParserRules.logicalExpression_return logicalExpression178 = null;


        org.eclipse.epsilon.common.parse.AST ob_tree=null;
        org.eclipse.epsilon.common.parse.AST cb_tree=null;

        try {
            // EolParserRules.g:558:2: (ob= '(' logicalExpression cb= ')' )
            // EolParserRules.g:558:4: ob= '(' logicalExpression cb= ')'
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            ob=(Token)match(input,104,FOLLOW_104_in_logicalExpressionInBrackets2570); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            ob_tree = (org.eclipse.epsilon.common.parse.AST)adaptor.create(ob);
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.becomeRoot(ob_tree, root_0);
            }
            pushFollow(FOLLOW_logicalExpression_in_logicalExpressionInBrackets2573);
            logicalExpression178=logicalExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, logicalExpression178.getTree());
            cb=(Token)match(input,105,FOLLOW_105_in_logicalExpressionInBrackets2577); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
              ob.setType(EXPRESSIONINBRACKETS);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(ob);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).getExtraTokens().add(cb);
              		((org.eclipse.epsilon.common.parse.AST)retval.tree).setImaginary(true);
              	
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end logicalExpressionInBrackets

    public static class literal_return extends ParserRuleReturnScope {
        org.eclipse.epsilon.common.parse.AST tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start literal
    // EolParserRules.g:562:1: literal : ( STRING | INT | FLOAT | BOOLEAN );
    public final Epl_EolParserRules.literal_return literal() throws RecognitionException {
        Epl_EolParserRules.literal_return retval = new Epl_EolParserRules.literal_return();
        retval.start = input.LT(1);

        org.eclipse.epsilon.common.parse.AST root_0 = null;

        Token set179=null;

        org.eclipse.epsilon.common.parse.AST set179_tree=null;

        try {
            // EolParserRules.g:563:2: ( STRING | INT | FLOAT | BOOLEAN )
            // EolParserRules.g:
            {
            root_0 = (org.eclipse.epsilon.common.parse.AST)adaptor.nil();

            set179=(Token)input.LT(1);
            if ( input.LA(1)==FLOAT||input.LA(1)==INT||input.LA(1)==BOOLEAN||input.LA(1)==STRING ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (org.eclipse.epsilon.common.parse.AST)adaptor.create(set179));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (org.eclipse.epsilon.common.parse.AST)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end literal

    // $ANTLR start synpred16_EolParserRules
    public final void synpred16_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:202:4: ( annotation )
        // EolParserRules.g:202:4: annotation
        {
        pushFollow(FOLLOW_annotation_in_synpred16_EolParserRules684);
        annotation();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred16_EolParserRules

    // $ANTLR start synpred29_EolParserRules
    public final void synpred29_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:255:4: ( ( '(' typeName ( ',' typeName )* ')' ) )
        // EolParserRules.g:255:4: ( '(' typeName ( ',' typeName )* ')' )
        {
        // EolParserRules.g:255:4: ( '(' typeName ( ',' typeName )* ')' )
        // EolParserRules.g:255:5: '(' typeName ( ',' typeName )* ')'
        {
        match(input,104,FOLLOW_104_in_synpred29_EolParserRules869); if (state.failed) return ;
        pushFollow(FOLLOW_typeName_in_synpred29_EolParserRules874);
        typeName();

        state._fsp--;
        if (state.failed) return ;
        // EolParserRules.g:255:50: ( ',' typeName )*
        loop80:
        do {
            int alt80=2;
            int LA80_0 = input.LA(1);

            if ( (LA80_0==97) ) {
                alt80=1;
            }


            switch (alt80) {
        	case 1 :
        	    // EolParserRules.g:255:51: ',' typeName
        	    {
        	    match(input,97,FOLLOW_97_in_synpred29_EolParserRules879); if (state.failed) return ;
        	    pushFollow(FOLLOW_typeName_in_synpred29_EolParserRules883);
        	    typeName();

        	    state._fsp--;
        	    if (state.failed) return ;

        	    }
        	    break;

        	default :
        	    break loop80;
            }
        } while (true);

        match(input,105,FOLLOW_105_in_synpred29_EolParserRules891); if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred29_EolParserRules

    // $ANTLR start synpred31_EolParserRules
    public final void synpred31_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:256:4: ( ( '<' typeName ( ',' typeName )* '>' ) )
        // EolParserRules.g:256:4: ( '<' typeName ( ',' typeName )* '>' )
        {
        // EolParserRules.g:256:4: ( '<' typeName ( ',' typeName )* '>' )
        // EolParserRules.g:256:5: '<' typeName ( ',' typeName )* '>'
        {
        match(input,120,FOLLOW_120_in_synpred31_EolParserRules903); if (state.failed) return ;
        pushFollow(FOLLOW_typeName_in_synpred31_EolParserRules908);
        typeName();

        state._fsp--;
        if (state.failed) return ;
        // EolParserRules.g:256:50: ( ',' typeName )*
        loop81:
        do {
            int alt81=2;
            int LA81_0 = input.LA(1);

            if ( (LA81_0==97) ) {
                alt81=1;
            }


            switch (alt81) {
        	case 1 :
        	    // EolParserRules.g:256:51: ',' typeName
        	    {
        	    match(input,97,FOLLOW_97_in_synpred31_EolParserRules913); if (state.failed) return ;
        	    pushFollow(FOLLOW_typeName_in_synpred31_EolParserRules917);
        	    typeName();

        	    state._fsp--;
        	    if (state.failed) return ;

        	    }
        	    break;

        	default :
        	    break loop81;
            }
        } while (true);

        match(input,121,FOLLOW_121_in_synpred31_EolParserRules925); if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred31_EolParserRules

    // $ANTLR start synpred32_EolParserRules
    public final void synpred32_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:261:4: ( statementA )
        // EolParserRules.g:261:4: statementA
        {
        pushFollow(FOLLOW_statementA_in_synpred32_EolParserRules944);
        statementA();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred32_EolParserRules

    // $ANTLR start synpred33_EolParserRules
    public final void synpred33_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:265:4: ( assignmentStatement )
        // EolParserRules.g:265:4: assignmentStatement
        {
        pushFollow(FOLLOW_assignmentStatement_in_synpred33_EolParserRules959);
        assignmentStatement();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred33_EolParserRules

    // $ANTLR start synpred34_EolParserRules
    public final void synpred34_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:265:26: ( expressionStatement )
        // EolParserRules.g:265:26: expressionStatement
        {
        pushFollow(FOLLOW_expressionStatement_in_synpred34_EolParserRules963);
        expressionStatement();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred34_EolParserRules

    // $ANTLR start synpred48_EolParserRules
    public final void synpred48_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:288:66: ( elseStatement )
        // EolParserRules.g:288:66: elseStatement
        {
        pushFollow(FOLLOW_elseStatement_in_synpred48_EolParserRules1118);
        elseStatement();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred48_EolParserRules

    // $ANTLR start synpred57_EolParserRules
    public final void synpred57_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:376:22: ( NAME ( ',' NAME )* )
        // EolParserRules.g:376:22: NAME ( ',' NAME )*
        {
        match(input,NAME,FOLLOW_NAME_in_synpred57_EolParserRules1499); if (state.failed) return ;
        // EolParserRules.g:376:27: ( ',' NAME )*
        loop82:
        do {
            int alt82=2;
            int LA82_0 = input.LA(1);

            if ( (LA82_0==97) ) {
                alt82=1;
            }


            switch (alt82) {
        	case 1 :
        	    // EolParserRules.g:376:28: ',' NAME
        	    {
        	    match(input,97,FOLLOW_97_in_synpred57_EolParserRules1502); if (state.failed) return ;
        	    match(input,NAME,FOLLOW_NAME_in_synpred57_EolParserRules1504); if (state.failed) return ;

        	    }
        	    break;

        	default :
        	    break loop82;
            }
        } while (true);


        }
    }
    // $ANTLR end synpred57_EolParserRules

    // $ANTLR start synpred63_EolParserRules
    public final void synpred63_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:392:5: ( postfixExpression '=' logicalExpression )
        // EolParserRules.g:392:5: postfixExpression '=' logicalExpression
        {
        pushFollow(FOLLOW_postfixExpression_in_synpred63_EolParserRules1596);
        postfixExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,101,FOLLOW_101_in_synpred63_EolParserRules1600); if (state.failed) return ;
        pushFollow(FOLLOW_logicalExpression_in_synpred63_EolParserRules1603);
        logicalExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred63_EolParserRules

    // $ANTLR start synpred74_EolParserRules
    public final void synpred74_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:401:24: ( ( '==' relationalExpression | '=' relationalExpression | ( '>' | '<' | '>=' | '<=' | '<>' ) additiveExpression ) )
        // EolParserRules.g:401:24: ( '==' relationalExpression | '=' relationalExpression | ( '>' | '<' | '>=' | '<=' | '<>' ) additiveExpression )
        {
        // EolParserRules.g:401:24: ( '==' relationalExpression | '=' relationalExpression | ( '>' | '<' | '>=' | '<=' | '<>' ) additiveExpression )
        int alt83=3;
        switch ( input.LA(1) ) {
        case 148:
            {
            alt83=1;
            }
            break;
        case 101:
            {
            alt83=2;
            }
            break;
        case 120:
        case 121:
        case 149:
        case 150:
        case 151:
            {
            alt83=3;
            }
            break;
        default:
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 83, 0, input);

            throw nvae;
        }

        switch (alt83) {
            case 1 :
                // EolParserRules.g:401:25: '==' relationalExpression
                {
                match(input,148,FOLLOW_148_in_synpred74_EolParserRules1674); if (state.failed) return ;
                pushFollow(FOLLOW_relationalExpression_in_synpred74_EolParserRules1677);
                relationalExpression();

                state._fsp--;
                if (state.failed) return ;

                }
                break;
            case 2 :
                // EolParserRules.g:401:57: '=' relationalExpression
                {
                match(input,101,FOLLOW_101_in_synpred74_EolParserRules1683); if (state.failed) return ;
                pushFollow(FOLLOW_relationalExpression_in_synpred74_EolParserRules1686);
                relationalExpression();

                state._fsp--;
                if (state.failed) return ;

                }
                break;
            case 3 :
                // EolParserRules.g:402:24: ( '>' | '<' | '>=' | '<=' | '<>' ) additiveExpression
                {
                if ( (input.LA(1)>=120 && input.LA(1)<=121)||(input.LA(1)>=149 && input.LA(1)<=151) ) {
                    input.consume();
                    state.errorRecovery=false;state.failed=false;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return ;}
                    MismatchedSetException mse = new MismatchedSetException(null,input);
                    throw mse;
                }

                pushFollow(FOLLOW_additiveExpression_in_synpred74_EolParserRules1740);
                additiveExpression();

                state._fsp--;
                if (state.failed) return ;

                }
                break;

        }


        }
    }
    // $ANTLR end synpred74_EolParserRules

    // $ANTLR start synpred94_EolParserRules
    public final void synpred94_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:464:4: ( NAME )
        // EolParserRules.g:464:4: NAME
        {
        match(input,NAME,FOLLOW_NAME_in_synpred94_EolParserRules2101); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred94_EolParserRules

    // $ANTLR start synpred104_EolParserRules
    public final void synpred104_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:495:30: ( ':' ( 'new' )? typeName ( parameterList )? )
        // EolParserRules.g:495:30: ':' ( 'new' )? typeName ( parameterList )?
        {
        match(input,106,FOLLOW_106_in_synpred104_EolParserRules2252); if (state.failed) return ;
        // EolParserRules.g:495:36: ( 'new' )?
        int alt87=2;
        int LA87_0 = input.LA(1);

        if ( (LA87_0==162) ) {
            alt87=1;
        }
        switch (alt87) {
            case 1 :
                // EolParserRules.g:0:0: 'new'
                {
                match(input,162,FOLLOW_162_in_synpred104_EolParserRules2257); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_typeName_in_synpred104_EolParserRules2263);
        typeName();

        state._fsp--;
        if (state.failed) return ;
        // EolParserRules.g:495:81: ( parameterList )?
        int alt88=2;
        int LA88_0 = input.LA(1);

        if ( (LA88_0==104) ) {
            alt88=1;
        }
        switch (alt88) {
            case 1 :
                // EolParserRules.g:0:0: parameterList
                {
                pushFollow(FOLLOW_parameterList_in_synpred104_EolParserRules2267);
                parameterList();

                state._fsp--;
                if (state.failed) return ;

                }
                break;

        }


        }
    }
    // $ANTLR end synpred104_EolParserRules

    // $ANTLR start synpred112_EolParserRules
    public final void synpred112_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:521:4: ( expressionRange )
        // EolParserRules.g:521:4: expressionRange
        {
        pushFollow(FOLLOW_expressionRange_in_synpred112_EolParserRules2403);
        expressionRange();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred112_EolParserRules

    // $ANTLR start synpred115_EolParserRules
    public final void synpred115_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:547:4: ( literalSequentialCollection )
        // EolParserRules.g:547:4: literalSequentialCollection
        {
        pushFollow(FOLLOW_literalSequentialCollection_in_synpred115_EolParserRules2511);
        literalSequentialCollection();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred115_EolParserRules

    // $ANTLR start synpred116_EolParserRules
    public final void synpred116_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:547:34: ( literalMapCollection )
        // EolParserRules.g:547:34: literalMapCollection
        {
        pushFollow(FOLLOW_literalMapCollection_in_synpred116_EolParserRules2515);
        literalMapCollection();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred116_EolParserRules

    // $ANTLR start synpred118_EolParserRules
    public final void synpred118_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:547:67: ( featureCall )
        // EolParserRules.g:547:67: featureCall
        {
        pushFollow(FOLLOW_featureCall_in_synpred118_EolParserRules2523);
        featureCall();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred118_EolParserRules

    // $ANTLR start synpred119_EolParserRules
    public final void synpred119_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:547:81: ( pathName )
        // EolParserRules.g:547:81: pathName
        {
        pushFollow(FOLLOW_pathName_in_synpred119_EolParserRules2527);
        pathName();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred119_EolParserRules

    // $ANTLR start synpred121_EolParserRules
    public final void synpred121_EolParserRules_fragment() throws RecognitionException {   
        // EolParserRules.g:548:5: ( collectionType )
        // EolParserRules.g:548:5: collectionType
        {
        pushFollow(FOLLOW_collectionType_in_synpred121_EolParserRules2537);
        collectionType();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred121_EolParserRules

    // Delegated rules

    public final boolean synpred33_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred33_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred112_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred112_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred57_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred57_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred63_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred63_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred31_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred31_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred94_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred94_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred16_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred16_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred74_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred74_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred121_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred121_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred29_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred29_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred115_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred115_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred119_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred119_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred104_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred104_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred118_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred118_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred34_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred34_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred32_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred32_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred48_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred48_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred116_EolParserRules() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred116_EolParserRules_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA22 dfa22 = new DFA22(this);
    protected DFA23 dfa23 = new DFA23(this);
    protected DFA24 dfa24 = new DFA24(this);
    protected DFA37 dfa37 = new DFA37(this);
    protected DFA40 dfa40 = new DFA40(this);
    protected DFA45 dfa45 = new DFA45(this);
    protected DFA56 dfa56 = new DFA56(this);
    protected DFA61 dfa61 = new DFA61(this);
    protected DFA72 dfa72 = new DFA72(this);
    protected DFA76 dfa76 = new DFA76(this);
    protected DFA79 dfa79 = new DFA79(this);
    static final String DFA22_eotS =
        "\67\uffff";
    static final String DFA22_eofS =
        "\1\3\66\uffff";
    static final String DFA22_minS =
        "\1\11\2\0\64\uffff";
    static final String DFA22_maxS =
        "\1\u00b1\2\0\64\uffff";
    static final String DFA22_acceptS =
        "\3\uffff\1\3\61\uffff\1\1\1\2";
    static final String DFA22_specialS =
        "\1\uffff\1\0\1\1\64\uffff}>";
    static final String[] DFA22_transitionS = {
            "\3\3\7\uffff\1\3\3\uffff\1\3\107\uffff\1\3\1\uffff\1\3\1\uffff"+
            "\5\3\1\1\2\3\1\uffff\1\3\13\uffff\1\2\1\3\1\uffff\1\3\16\uffff"+
            "\22\3\1\uffff\5\3\3\uffff\3\3\1\uffff\1\3\1\uffff\7\3",
            "\1\uffff",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA22_eot = DFA.unpackEncodedString(DFA22_eotS);
    static final short[] DFA22_eof = DFA.unpackEncodedString(DFA22_eofS);
    static final char[] DFA22_min = DFA.unpackEncodedStringToUnsignedChars(DFA22_minS);
    static final char[] DFA22_max = DFA.unpackEncodedStringToUnsignedChars(DFA22_maxS);
    static final short[] DFA22_accept = DFA.unpackEncodedString(DFA22_acceptS);
    static final short[] DFA22_special = DFA.unpackEncodedString(DFA22_specialS);
    static final short[][] DFA22_transition;

    static {
        int numStates = DFA22_transitionS.length;
        DFA22_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA22_transition[i] = DFA.unpackEncodedString(DFA22_transitionS[i]);
        }
    }

    class DFA22 extends DFA {

        public DFA22(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 22;
            this.eot = DFA22_eot;
            this.eof = DFA22_eof;
            this.min = DFA22_min;
            this.max = DFA22_max;
            this.accept = DFA22_accept;
            this.special = DFA22_special;
            this.transition = DFA22_transition;
        }
        public String getDescription() {
            return "255:3: ( (op= '(' tn= typeName ( ',' tn= typeName )* cp= ')' ) | (op= '<' tn= typeName ( ',' tn= typeName )* cp= '>' ) )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA22_1 = input.LA(1);

                         
                        int index22_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_EolParserRules()) ) {s = 53;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index22_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA22_2 = input.LA(1);

                         
                        int index22_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred31_EolParserRules()) ) {s = 54;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index22_2);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 22, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA23_eotS =
        "\35\uffff";
    static final String DFA23_eofS =
        "\35\uffff";
    static final String DFA23_minS =
        "\1\4\24\uffff\1\0\7\uffff";
    static final String DFA23_maxS =
        "\1\u00a4\24\uffff\1\0\7\uffff";
    static final String DFA23_acceptS =
        "\1\uffff\1\1\25\uffff\1\2\5\uffff";
    static final String DFA23_specialS =
        "\25\uffff\1\0\7\uffff}>";
    static final String[] DFA23_transitionS = {
            "\1\1\3\uffff\1\1\3\uffff\1\1\1\uffff\1\1\4\uffff\1\1\124\uffff"+
            "\1\1\7\uffff\10\1\2\uffff\1\1\1\uffff\2\1\3\uffff\1\1\1\25\2"+
            "\27\1\1\4\27\17\uffff\1\1\2\uffff\1\1\5\uffff\3\1",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA23_eot = DFA.unpackEncodedString(DFA23_eotS);
    static final short[] DFA23_eof = DFA.unpackEncodedString(DFA23_eofS);
    static final char[] DFA23_min = DFA.unpackEncodedStringToUnsignedChars(DFA23_minS);
    static final char[] DFA23_max = DFA.unpackEncodedStringToUnsignedChars(DFA23_maxS);
    static final short[] DFA23_accept = DFA.unpackEncodedString(DFA23_acceptS);
    static final short[] DFA23_special = DFA.unpackEncodedString(DFA23_specialS);
    static final short[][] DFA23_transition;

    static {
        int numStates = DFA23_transitionS.length;
        DFA23_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA23_transition[i] = DFA.unpackEncodedString(DFA23_transitionS[i]);
        }
    }

    class DFA23 extends DFA {

        public DFA23(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 23;
            this.eot = DFA23_eot;
            this.eof = DFA23_eof;
            this.min = DFA23_min;
            this.max = DFA23_max;
            this.accept = DFA23_accept;
            this.special = DFA23_special;
            this.transition = DFA23_transition;
        }
        public String getDescription() {
            return "260:1: statement : ( statementA | statementB );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA23_21 = input.LA(1);

                         
                        int index23_21 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_EolParserRules()) ) {s = 1;}

                        else if ( (true) ) {s = 23;}

                         
                        input.seek(index23_21);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 23, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA24_eotS =
        "\31\uffff";
    static final String DFA24_eofS =
        "\31\uffff";
    static final String DFA24_minS =
        "\1\4\20\0\10\uffff";
    static final String DFA24_maxS =
        "\1\u00a4\20\0\10\uffff";
    static final String DFA24_acceptS =
        "\21\uffff\1\3\1\4\1\5\1\6\1\7\1\10\1\1\1\2";
    static final String DFA24_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\1\16\1\17\10\uffff}>";
    static final String[] DFA24_transitionS = {
            "\1\12\3\uffff\1\12\3\uffff\1\12\1\uffff\1\12\4\uffff\1\13\124"+
            "\uffff\1\15\7\uffff\1\14\1\3\1\4\1\5\1\6\1\7\1\10\1\11\2\uffff"+
            "\1\21\1\uffff\1\22\1\24\3\uffff\1\23\1\25\2\uffff\1\26\23\uffff"+
            "\1\2\2\uffff\1\1\5\uffff\1\16\1\17\1\20",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA24_eot = DFA.unpackEncodedString(DFA24_eotS);
    static final short[] DFA24_eof = DFA.unpackEncodedString(DFA24_eofS);
    static final char[] DFA24_min = DFA.unpackEncodedStringToUnsignedChars(DFA24_minS);
    static final char[] DFA24_max = DFA.unpackEncodedStringToUnsignedChars(DFA24_maxS);
    static final short[] DFA24_accept = DFA.unpackEncodedString(DFA24_acceptS);
    static final short[] DFA24_special = DFA.unpackEncodedString(DFA24_specialS);
    static final short[][] DFA24_transition;

    static {
        int numStates = DFA24_transitionS.length;
        DFA24_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA24_transition[i] = DFA.unpackEncodedString(DFA24_transitionS[i]);
        }
    }

    class DFA24 extends DFA {

        public DFA24(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 24;
            this.eot = DFA24_eot;
            this.eof = DFA24_eof;
            this.min = DFA24_min;
            this.max = DFA24_max;
            this.accept = DFA24_accept;
            this.special = DFA24_special;
            this.transition = DFA24_transition;
        }
        public String getDescription() {
            return "264:1: statementA : ( assignmentStatement | expressionStatement | forStatement | ifStatement | whileStatement | switchStatement | returnStatement | breakStatement );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA24_1 = input.LA(1);

                         
                        int index24_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA24_2 = input.LA(1);

                         
                        int index24_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA24_3 = input.LA(1);

                         
                        int index24_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA24_4 = input.LA(1);

                         
                        int index24_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA24_5 = input.LA(1);

                         
                        int index24_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA24_6 = input.LA(1);

                         
                        int index24_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA24_7 = input.LA(1);

                         
                        int index24_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA24_8 = input.LA(1);

                         
                        int index24_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA24_9 = input.LA(1);

                         
                        int index24_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA24_10 = input.LA(1);

                         
                        int index24_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA24_11 = input.LA(1);

                         
                        int index24_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA24_12 = input.LA(1);

                         
                        int index24_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA24_13 = input.LA(1);

                         
                        int index24_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA24_14 = input.LA(1);

                         
                        int index24_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_14);
                        if ( s>=0 ) return s;
                        break;
                    case 14 : 
                        int LA24_15 = input.LA(1);

                         
                        int index24_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_15);
                        if ( s>=0 ) return s;
                        break;
                    case 15 : 
                        int LA24_16 = input.LA(1);

                         
                        int index24_16 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_EolParserRules()) ) {s = 23;}

                        else if ( (synpred34_EolParserRules()) ) {s = 24;}

                         
                        input.seek(index24_16);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 24, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA37_eotS =
        "\37\uffff";
    static final String DFA37_eofS =
        "\37\uffff";
    static final String DFA37_minS =
        "\1\4\1\0\35\uffff";
    static final String DFA37_maxS =
        "\1\u00a4\1\0\35\uffff";
    static final String DFA37_acceptS =
        "\2\uffff\1\2\33\uffff\1\1";
    static final String DFA37_specialS =
        "\1\uffff\1\0\35\uffff}>";
    static final String[] DFA37_transitionS = {
            "\1\2\3\uffff\1\2\3\uffff\1\2\1\uffff\1\2\4\uffff\1\1\117\uffff"+
            "\1\2\4\uffff\1\2\7\uffff\10\2\2\uffff\1\2\1\uffff\2\2\3\uffff"+
            "\11\2\17\uffff\1\2\2\uffff\1\2\5\uffff\3\2",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA37_eot = DFA.unpackEncodedString(DFA37_eotS);
    static final short[] DFA37_eof = DFA.unpackEncodedString(DFA37_eofS);
    static final char[] DFA37_min = DFA.unpackEncodedStringToUnsignedChars(DFA37_minS);
    static final char[] DFA37_max = DFA.unpackEncodedStringToUnsignedChars(DFA37_maxS);
    static final short[] DFA37_accept = DFA.unpackEncodedString(DFA37_acceptS);
    static final short[] DFA37_special = DFA.unpackEncodedString(DFA37_specialS);
    static final short[][] DFA37_transition;

    static {
        int numStates = DFA37_transitionS.length;
        DFA37_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA37_transition[i] = DFA.unpackEncodedString(DFA37_transitionS[i]);
        }
    }

    class DFA37 extends DFA {

        public DFA37(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 37;
            this.eot = DFA37_eot;
            this.eof = DFA37_eof;
            this.min = DFA37_min;
            this.max = DFA37_max;
            this.accept = DFA37_accept;
            this.special = DFA37_special;
            this.transition = DFA37_transition;
        }
        public String getDescription() {
            return "376:21: ( NAME ( ',' NAME )* )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA37_1 = input.LA(1);

                         
                        int index37_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred57_EolParserRules()) ) {s = 30;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index37_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 37, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA40_eotS =
        "\22\uffff";
    static final String DFA40_eofS =
        "\22\uffff";
    static final String DFA40_minS =
        "\1\4\16\0\3\uffff";
    static final String DFA40_maxS =
        "\1\u00a4\16\0\3\uffff";
    static final String DFA40_acceptS =
        "\17\uffff\1\2\1\uffff\1\1";
    static final String DFA40_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\3\uffff}>";
    static final String[] DFA40_transitionS = {
            "\1\10\3\uffff\1\10\3\uffff\1\10\1\uffff\1\10\4\uffff\1\11\124"+
            "\uffff\1\13\7\uffff\1\12\1\1\1\2\1\3\1\4\1\5\1\6\1\7\41\uffff"+
            "\1\17\2\uffff\1\17\5\uffff\1\14\1\15\1\16",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            "",
            ""
    };

    static final short[] DFA40_eot = DFA.unpackEncodedString(DFA40_eotS);
    static final short[] DFA40_eof = DFA.unpackEncodedString(DFA40_eofS);
    static final char[] DFA40_min = DFA.unpackEncodedStringToUnsignedChars(DFA40_minS);
    static final char[] DFA40_max = DFA.unpackEncodedStringToUnsignedChars(DFA40_maxS);
    static final short[] DFA40_accept = DFA.unpackEncodedString(DFA40_acceptS);
    static final short[] DFA40_special = DFA.unpackEncodedString(DFA40_specialS);
    static final short[][] DFA40_transition;

    static {
        int numStates = DFA40_transitionS.length;
        DFA40_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA40_transition[i] = DFA.unpackEncodedString(DFA40_transitionS[i]);
        }
    }

    class DFA40 extends DFA {

        public DFA40(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 40;
            this.eot = DFA40_eot;
            this.eof = DFA40_eof;
            this.min = DFA40_min;
            this.max = DFA40_max;
            this.accept = DFA40_accept;
            this.special = DFA40_special;
            this.transition = DFA40_transition;
        }
        public String getDescription() {
            return "392:4: ( postfixExpression op= '=' logicalExpression | logicalExpression )";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA40_1 = input.LA(1);

                         
                        int index40_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA40_2 = input.LA(1);

                         
                        int index40_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA40_3 = input.LA(1);

                         
                        int index40_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA40_4 = input.LA(1);

                         
                        int index40_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA40_5 = input.LA(1);

                         
                        int index40_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA40_6 = input.LA(1);

                         
                        int index40_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA40_7 = input.LA(1);

                         
                        int index40_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA40_8 = input.LA(1);

                         
                        int index40_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA40_9 = input.LA(1);

                         
                        int index40_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA40_10 = input.LA(1);

                         
                        int index40_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA40_11 = input.LA(1);

                         
                        int index40_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA40_12 = input.LA(1);

                         
                        int index40_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA40_13 = input.LA(1);

                         
                        int index40_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA40_14 = input.LA(1);

                         
                        int index40_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred63_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index40_14);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 40, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA45_eotS =
        "\12\uffff";
    static final String DFA45_eofS =
        "\1\1\11\uffff";
    static final String DFA45_minS =
        "\1\12\1\uffff\7\0\1\uffff";
    static final String DFA45_maxS =
        "\1\u00b1\1\uffff\7\0\1\uffff";
    static final String DFA45_acceptS =
        "\1\uffff\1\2\7\uffff\1\1";
    static final String DFA45_specialS =
        "\2\uffff\1\1\1\0\1\2\1\5\1\3\1\6\1\4\1\uffff}>";
    static final String[] DFA45_transitionS = {
            "\1\1\14\uffff\1\1\107\uffff\1\1\1\uffff\1\1\1\uffff\2\1\1\3"+
            "\2\1\1\uffff\2\1\1\uffff\1\1\13\uffff\1\5\1\4\1\uffff\1\1\16"+
            "\uffff\12\1\1\2\1\6\1\7\1\10\7\uffff\1\1\5\uffff\3\1\1\uffff"+
            "\1\1\1\uffff\7\1",
            "",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            ""
    };

    static final short[] DFA45_eot = DFA.unpackEncodedString(DFA45_eotS);
    static final short[] DFA45_eof = DFA.unpackEncodedString(DFA45_eofS);
    static final char[] DFA45_min = DFA.unpackEncodedStringToUnsignedChars(DFA45_minS);
    static final char[] DFA45_max = DFA.unpackEncodedStringToUnsignedChars(DFA45_maxS);
    static final short[] DFA45_accept = DFA.unpackEncodedString(DFA45_acceptS);
    static final short[] DFA45_special = DFA.unpackEncodedString(DFA45_specialS);
    static final short[][] DFA45_transition;

    static {
        int numStates = DFA45_transitionS.length;
        DFA45_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA45_transition[i] = DFA.unpackEncodedString(DFA45_transitionS[i]);
        }
    }

    class DFA45 extends DFA {

        public DFA45(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 45;
            this.eot = DFA45_eot;
            this.eof = DFA45_eof;
            this.min = DFA45_min;
            this.max = DFA45_max;
            this.accept = DFA45_accept;
            this.special = DFA45_special;
            this.transition = DFA45_transition;
        }
        public String getDescription() {
            return "()* loopback of 401:23: ( (op= '==' relationalExpression | op= '=' relationalExpression | (op= '>' | op= '<' | op= '>=' | op= '<=' | op= '<>' ) additiveExpression ) )*";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA45_3 = input.LA(1);

                         
                        int index45_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred74_EolParserRules()) ) {s = 9;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index45_3);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA45_2 = input.LA(1);

                         
                        int index45_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred74_EolParserRules()) ) {s = 9;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index45_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA45_4 = input.LA(1);

                         
                        int index45_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred74_EolParserRules()) ) {s = 9;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index45_4);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA45_6 = input.LA(1);

                         
                        int index45_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred74_EolParserRules()) ) {s = 9;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index45_6);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA45_8 = input.LA(1);

                         
                        int index45_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred74_EolParserRules()) ) {s = 9;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index45_8);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA45_5 = input.LA(1);

                         
                        int index45_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred74_EolParserRules()) ) {s = 9;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index45_5);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA45_7 = input.LA(1);

                         
                        int index45_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred74_EolParserRules()) ) {s = 9;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index45_7);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 45, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA56_eotS =
        "\44\uffff";
    static final String DFA56_eofS =
        "\1\uffff\1\3\42\uffff";
    static final String DFA56_minS =
        "\1\23\1\11\1\4\1\uffff\1\11\1\uffff\1\11\1\150\3\23\2\4\1\16\5\11"+
        "\1\150\1\4\1\11\1\151\1\23\1\4\2\23\1\4\1\16\5\11\1\151\1\11";
    static final String DFA56_maxS =
        "\1\23\1\u00b1\1\u00a4\1\uffff\1\u00a1\1\uffff\1\u009e\1\150\3\23"+
        "\2\u00a4\1\16\3\u009e\1\u00a1\1\u009e\1\150\1\u00a4\1\u009e\1\151"+
        "\1\23\1\u00a4\2\23\1\u00a4\1\16\1\u00a1\4\u009e\1\151\1\u009e";
    static final String DFA56_acceptS =
        "\3\uffff\1\1\1\uffff\1\2\36\uffff";
    static final String DFA56_specialS =
        "\44\uffff}>";
    static final String[] DFA56_transitionS = {
            "\1\1",
            "\3\3\13\uffff\1\3\107\uffff\1\3\1\uffff\1\3\1\uffff\5\3\1\2"+
            "\2\3\1\uffff\1\3\13\uffff\2\3\1\uffff\1\3\16\uffff\22\3\1\uffff"+
            "\3\3\5\uffff\3\3\1\uffff\1\3\1\uffff\7\3",
            "\1\6\3\uffff\1\6\3\uffff\1\6\1\uffff\1\6\4\uffff\1\4\124\uffff"+
            "\2\3\6\uffff\1\7\7\3\41\uffff\1\3\2\uffff\1\3\3\uffff\2\5\3"+
            "\3",
            "",
            "\1\3\1\uffff\1\3\125\uffff\1\13\3\uffff\1\3\2\uffff\2\3\1\5"+
            "\2\uffff\1\10\1\12\1\11\10\uffff\2\3\26\uffff\14\3\1\uffff\2"+
            "\3\1\uffff\2\5",
            "",
            "\1\3\1\uffff\1\3\125\uffff\1\14\3\uffff\1\3\3\uffff\1\3\16\uffff"+
            "\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\15",
            "\1\16",
            "\1\17",
            "\1\20",
            "\1\22\3\uffff\1\22\3\uffff\1\22\1\uffff\1\22\4\uffff\1\21\124"+
            "\uffff\1\24\7\uffff\1\23\7\3\41\uffff\1\3\2\uffff\1\3\1\uffff"+
            "\1\5\3\uffff\3\3",
            "\1\22\3\uffff\1\22\3\uffff\1\22\1\uffff\1\22\4\uffff\1\25\124"+
            "\uffff\1\24\7\uffff\1\23\7\3\41\uffff\1\3\2\uffff\1\3\1\uffff"+
            "\1\5\3\uffff\3\3",
            "\1\26",
            "\1\3\1\uffff\1\3\125\uffff\1\14\3\uffff\1\3\3\uffff\1\3\4\uffff"+
            "\1\12\1\11\10\uffff\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\3\1\uffff\1\3\125\uffff\1\14\3\uffff\1\3\3\uffff\1\3\4\uffff"+
            "\1\12\1\11\10\uffff\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\3\1\uffff\1\3\125\uffff\1\14\3\uffff\1\3\3\uffff\1\3\16\uffff"+
            "\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\3\1\uffff\1\3\125\uffff\1\30\3\uffff\1\3\2\uffff\2\3\1\5"+
            "\2\uffff\1\27\1\32\1\31\10\uffff\2\3\26\uffff\14\3\1\uffff\2"+
            "\3\1\uffff\2\5",
            "\1\3\1\uffff\1\3\125\uffff\1\33\3\uffff\1\3\3\uffff\1\3\16\uffff"+
            "\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\34",
            "\1\3\3\uffff\1\3\3\uffff\1\3\1\uffff\1\3\4\uffff\1\35\124\uffff"+
            "\1\3\7\uffff\10\3\41\uffff\1\3\2\uffff\1\3\3\uffff\2\5\3\3",
            "\1\3\1\uffff\1\3\125\uffff\1\33\3\uffff\1\3\2\uffff\2\3\3\uffff"+
            "\1\27\1\32\1\31\10\uffff\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\36",
            "\1\37",
            "\1\22\3\uffff\1\22\3\uffff\1\22\1\uffff\1\22\4\uffff\1\21\124"+
            "\uffff\1\24\7\uffff\1\23\7\3\41\uffff\1\3\2\uffff\1\3\1\uffff"+
            "\1\5\3\uffff\3\3",
            "\1\40",
            "\1\41",
            "\1\22\3\uffff\1\22\3\uffff\1\22\1\uffff\1\22\4\uffff\1\25\124"+
            "\uffff\1\24\7\uffff\1\23\7\3\41\uffff\1\3\2\uffff\1\3\1\uffff"+
            "\1\5\3\uffff\3\3",
            "\1\42",
            "\1\3\1\uffff\1\3\125\uffff\1\5\3\uffff\1\3\2\uffff\2\3\1\5\2"+
            "\uffff\3\3\10\uffff\2\3\26\uffff\14\3\1\uffff\2\3\1\uffff\2"+
            "\5",
            "\1\3\1\uffff\1\3\125\uffff\1\14\3\uffff\1\3\3\uffff\1\3\16\uffff"+
            "\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\3\1\uffff\1\3\125\uffff\1\33\3\uffff\1\3\3\uffff\1\3\4\uffff"+
            "\1\32\1\31\10\uffff\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\3\1\uffff\1\3\125\uffff\1\33\3\uffff\1\3\3\uffff\1\3\4\uffff"+
            "\1\32\1\31\10\uffff\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\3\1\uffff\1\3\125\uffff\1\33\3\uffff\1\3\3\uffff\1\3\16\uffff"+
            "\2\3\26\uffff\14\3\1\uffff\2\3",
            "\1\43",
            "\1\3\1\uffff\1\3\125\uffff\1\33\3\uffff\1\3\3\uffff\1\3\16\uffff"+
            "\2\3\26\uffff\14\3\1\uffff\2\3"
    };

    static final short[] DFA56_eot = DFA.unpackEncodedString(DFA56_eotS);
    static final short[] DFA56_eof = DFA.unpackEncodedString(DFA56_eofS);
    static final char[] DFA56_min = DFA.unpackEncodedStringToUnsignedChars(DFA56_minS);
    static final char[] DFA56_max = DFA.unpackEncodedStringToUnsignedChars(DFA56_maxS);
    static final short[] DFA56_accept = DFA.unpackEncodedString(DFA56_acceptS);
    static final short[] DFA56_special = DFA.unpackEncodedString(DFA56_specialS);
    static final short[][] DFA56_transition;

    static {
        int numStates = DFA56_transitionS.length;
        DFA56_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA56_transition[i] = DFA.unpackEncodedString(DFA56_transitionS[i]);
        }
    }

    class DFA56 extends DFA {

        public DFA56(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 56;
            this.eot = DFA56_eot;
            this.eof = DFA56_eof;
            this.min = DFA56_min;
            this.max = DFA56_max;
            this.accept = DFA56_accept;
            this.special = DFA56_special;
            this.transition = DFA56_transition;
        }
        public String getDescription() {
            return "435:1: featureCall : ( simpleFeatureCall | complexFeatureCall );";
        }
    }
    static final String DFA61_eotS =
        "\7\uffff";
    static final String DFA61_eofS =
        "\7\uffff";
    static final String DFA61_minS =
        "\1\4\1\141\2\uffff\1\4\1\141\1\4";
    static final String DFA61_maxS =
        "\2\u00a1\2\uffff\1\u009e\1\u00a1\1\u009e";
    static final String DFA61_acceptS =
        "\2\uffff\1\1\1\2\3\uffff";
    static final String DFA61_specialS =
        "\7\uffff}>";
    static final String[] DFA61_transitionS = {
            "\1\3\3\uffff\1\3\3\uffff\1\3\1\uffff\1\3\4\uffff\1\1\134\uffff"+
            "\1\3\57\uffff\2\2",
            "\1\4\10\uffff\1\2\2\uffff\3\3\60\uffff\2\2",
            "",
            "",
            "\1\3\3\uffff\1\3\3\uffff\1\3\1\uffff\1\3\4\uffff\1\5\124\uffff"+
            "\1\3\7\uffff\1\3\55\uffff\1\3",
            "\1\6\10\uffff\1\2\2\uffff\3\3\60\uffff\2\2",
            "\1\3\3\uffff\1\3\3\uffff\1\3\1\uffff\1\3\4\uffff\1\5\124\uffff"+
            "\1\3\7\uffff\1\3\55\uffff\1\3"
    };

    static final short[] DFA61_eot = DFA.unpackEncodedString(DFA61_eotS);
    static final short[] DFA61_eof = DFA.unpackEncodedString(DFA61_eofS);
    static final char[] DFA61_min = DFA.unpackEncodedStringToUnsignedChars(DFA61_minS);
    static final char[] DFA61_max = DFA.unpackEncodedStringToUnsignedChars(DFA61_maxS);
    static final short[] DFA61_accept = DFA.unpackEncodedString(DFA61_acceptS);
    static final short[] DFA61_special = DFA.unpackEncodedString(DFA61_specialS);
    static final short[][] DFA61_transition;

    static {
        int numStates = DFA61_transitionS.length;
        DFA61_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA61_transition[i] = DFA.unpackEncodedString(DFA61_transitionS[i]);
        }
    }

    class DFA61 extends DFA {

        public DFA61(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 61;
            this.eot = DFA61_eot;
            this.eof = DFA61_eof;
            this.min = DFA61_min;
            this.max = DFA61_max;
            this.accept = DFA61_accept;
            this.special = DFA61_special;
            this.transition = DFA61_transition;
        }
        public String getDescription() {
            return "459:18: ( lambdaExpression | ( complexParameterExpression ',' ( complexParameterExpression ',' )* lambdaExpressionInBrackets ) )";
        }
    }
    static final String DFA72_eotS =
        "\27\uffff";
    static final String DFA72_eofS =
        "\2\2\25\uffff";
    static final String DFA72_minS =
        "\1\11\1\4\1\uffff\1\23\1\0\1\150\10\0\1\150\1\0\1\uffff\2\16\2\151"+
        "\2\0";
    static final String DFA72_maxS =
        "\1\u00b1\1\u00a4\1\uffff\1\167\1\0\1\150\10\0\1\150\1\0\1\uffff"+
        "\2\16\2\151\2\0";
    static final String DFA72_acceptS =
        "\2\uffff\1\2\15\uffff\1\1\6\uffff";
    static final String DFA72_specialS =
        "\4\uffff\1\11\1\uffff\1\2\1\7\1\12\1\5\1\6\1\10\1\1\1\13\1\uffff"+
        "\1\3\5\uffff\1\0\1\4}>";
    static final String[] DFA72_transitionS = {
            "\3\2\13\uffff\1\2\107\uffff\1\2\1\uffff\1\2\1\uffff\5\2\1\uffff"+
            "\1\2\1\1\1\uffff\1\2\13\uffff\2\2\1\uffff\1\2\16\uffff\22\2"+
            "\1\uffff\3\2\5\uffff\3\2\1\uffff\1\2\1\uffff\7\2",
            "\1\2\3\uffff\1\2\3\uffff\1\2\1\uffff\1\2\4\uffff\1\4\117\uffff"+
            "\2\2\3\uffff\1\2\7\uffff\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
            "\2\uffff\1\2\1\uffff\4\2\1\uffff\11\2\17\uffff\1\2\2\uffff\1"+
            "\2\5\uffff\1\3\2\2",
            "",
            "\1\15\134\uffff\1\16\7\17",
            "\1\uffff",
            "\1\21",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\22",
            "\1\uffff",
            "",
            "\1\23",
            "\1\24",
            "\1\25",
            "\1\26",
            "\1\uffff",
            "\1\uffff"
    };

    static final short[] DFA72_eot = DFA.unpackEncodedString(DFA72_eotS);
    static final short[] DFA72_eof = DFA.unpackEncodedString(DFA72_eofS);
    static final char[] DFA72_min = DFA.unpackEncodedStringToUnsignedChars(DFA72_minS);
    static final char[] DFA72_max = DFA.unpackEncodedStringToUnsignedChars(DFA72_maxS);
    static final short[] DFA72_accept = DFA.unpackEncodedString(DFA72_acceptS);
    static final short[] DFA72_special = DFA.unpackEncodedString(DFA72_specialS);
    static final short[][] DFA72_transition;

    static {
        int numStates = DFA72_transitionS.length;
        DFA72_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA72_transition[i] = DFA.unpackEncodedString(DFA72_transitionS[i]);
        }
    }

    class DFA72 extends DFA {

        public DFA72(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 72;
            this.eot = DFA72_eot;
            this.eof = DFA72_eof;
            this.min = DFA72_min;
            this.max = DFA72_max;
            this.accept = DFA72_accept;
            this.special = DFA72_special;
            this.transition = DFA72_transition;
        }
        public String getDescription() {
            return "495:29: ( ':' (n= 'new' )? t= typeName ( parameterList )? )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA72_21 = input.LA(1);

                         
                        int index72_21 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_21);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA72_12 = input.LA(1);

                         
                        int index72_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_12);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA72_6 = input.LA(1);

                         
                        int index72_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_6);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA72_15 = input.LA(1);

                         
                        int index72_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_15);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA72_22 = input.LA(1);

                         
                        int index72_22 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_22);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA72_9 = input.LA(1);

                         
                        int index72_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_9);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA72_10 = input.LA(1);

                         
                        int index72_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_10);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA72_7 = input.LA(1);

                         
                        int index72_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_7);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA72_11 = input.LA(1);

                         
                        int index72_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_11);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA72_4 = input.LA(1);

                         
                        int index72_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_4);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA72_8 = input.LA(1);

                         
                        int index72_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_8);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA72_13 = input.LA(1);

                         
                        int index72_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred104_EolParserRules()) ) {s = 16;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index72_13);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 72, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA76_eotS =
        "\23\uffff";
    static final String DFA76_eofS =
        "\23\uffff";
    static final String DFA76_minS =
        "\1\4\20\0\2\uffff";
    static final String DFA76_maxS =
        "\1\u00a4\20\0\2\uffff";
    static final String DFA76_acceptS =
        "\21\uffff\1\1\1\2";
    static final String DFA76_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\1\16\1\17\2\uffff}>";
    static final String[] DFA76_transitionS = {
            "\1\12\3\uffff\1\12\3\uffff\1\12\1\uffff\1\12\4\uffff\1\13\124"+
            "\uffff\1\15\7\uffff\1\14\1\3\1\4\1\5\1\6\1\7\1\10\1\11\41\uffff"+
            "\1\2\2\uffff\1\1\5\uffff\1\16\1\17\1\20",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            ""
    };

    static final short[] DFA76_eot = DFA.unpackEncodedString(DFA76_eotS);
    static final short[] DFA76_eof = DFA.unpackEncodedString(DFA76_eofS);
    static final char[] DFA76_min = DFA.unpackEncodedStringToUnsignedChars(DFA76_minS);
    static final char[] DFA76_max = DFA.unpackEncodedStringToUnsignedChars(DFA76_maxS);
    static final short[] DFA76_accept = DFA.unpackEncodedString(DFA76_acceptS);
    static final short[] DFA76_special = DFA.unpackEncodedString(DFA76_specialS);
    static final short[][] DFA76_transition;

    static {
        int numStates = DFA76_transitionS.length;
        DFA76_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA76_transition[i] = DFA.unpackEncodedString(DFA76_transitionS[i]);
        }
    }

    class DFA76 extends DFA {

        public DFA76(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 76;
            this.eot = DFA76_eot;
            this.eof = DFA76_eof;
            this.min = DFA76_min;
            this.max = DFA76_max;
            this.accept = DFA76_accept;
            this.special = DFA76_special;
            this.transition = DFA76_transition;
        }
        public String getDescription() {
            return "520:1: expressionListOrRange : ( expressionRange | expressionList );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA76_1 = input.LA(1);

                         
                        int index76_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA76_2 = input.LA(1);

                         
                        int index76_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA76_3 = input.LA(1);

                         
                        int index76_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA76_4 = input.LA(1);

                         
                        int index76_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA76_5 = input.LA(1);

                         
                        int index76_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA76_6 = input.LA(1);

                         
                        int index76_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA76_7 = input.LA(1);

                         
                        int index76_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA76_8 = input.LA(1);

                         
                        int index76_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA76_9 = input.LA(1);

                         
                        int index76_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA76_10 = input.LA(1);

                         
                        int index76_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA76_11 = input.LA(1);

                         
                        int index76_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA76_12 = input.LA(1);

                         
                        int index76_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA76_13 = input.LA(1);

                         
                        int index76_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA76_14 = input.LA(1);

                         
                        int index76_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_14);
                        if ( s>=0 ) return s;
                        break;
                    case 14 : 
                        int LA76_15 = input.LA(1);

                         
                        int index76_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_15);
                        if ( s>=0 ) return s;
                        break;
                    case 15 : 
                        int LA76_16 = input.LA(1);

                         
                        int index76_16 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred112_EolParserRules()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index76_16);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 76, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA79_eotS =
        "\24\uffff";
    static final String DFA79_eofS =
        "\24\uffff";
    static final String DFA79_minS =
        "\1\4\7\0\1\uffff\1\0\12\uffff";
    static final String DFA79_maxS =
        "\1\u00a4\7\0\1\uffff\1\0\12\uffff";
    static final String DFA79_acceptS =
        "\10\uffff\1\3\1\uffff\1\6\1\10\1\11\1\12\1\uffff\1\1\1\7\1\2\1\4"+
        "\1\5";
    static final String DFA79_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\uffff\1\7\12\uffff}>";
    static final String[] DFA79_transitionS = {
            "\1\10\3\uffff\1\10\3\uffff\1\10\1\uffff\1\10\4\uffff\1\11\124"+
            "\uffff\1\13\7\uffff\1\12\1\1\1\2\1\3\1\4\1\5\1\6\1\7\52\uffff"+
            "\1\14\2\15",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA79_eot = DFA.unpackEncodedString(DFA79_eotS);
    static final short[] DFA79_eof = DFA.unpackEncodedString(DFA79_eofS);
    static final char[] DFA79_min = DFA.unpackEncodedStringToUnsignedChars(DFA79_minS);
    static final char[] DFA79_max = DFA.unpackEncodedStringToUnsignedChars(DFA79_maxS);
    static final short[] DFA79_accept = DFA.unpackEncodedString(DFA79_acceptS);
    static final short[] DFA79_special = DFA.unpackEncodedString(DFA79_specialS);
    static final short[][] DFA79_transition;

    static {
        int numStates = DFA79_transitionS.length;
        DFA79_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA79_transition[i] = DFA.unpackEncodedString(DFA79_transitionS[i]);
        }
    }

    class DFA79 extends DFA {

        public DFA79(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 79;
            this.eot = DFA79_eot;
            this.eof = DFA79_eof;
            this.min = DFA79_min;
            this.max = DFA79_max;
            this.accept = DFA79_accept;
            this.special = DFA79_special;
            this.transition = DFA79_transition;
        }
        public String getDescription() {
            return "546:1: primitiveExpression : ( literalSequentialCollection | literalMapCollection | literal | featureCall | pathName | nativeType | collectionType | logicalExpressionInBrackets | newExpression | variableDeclarationExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA79_1 = input.LA(1);

                         
                        int index79_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred115_EolParserRules()) ) {s = 15;}

                        else if ( (synpred121_EolParserRules()) ) {s = 16;}

                         
                        input.seek(index79_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA79_2 = input.LA(1);

                         
                        int index79_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred115_EolParserRules()) ) {s = 15;}

                        else if ( (synpred121_EolParserRules()) ) {s = 16;}

                         
                        input.seek(index79_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA79_3 = input.LA(1);

                         
                        int index79_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred115_EolParserRules()) ) {s = 15;}

                        else if ( (synpred121_EolParserRules()) ) {s = 16;}

                         
                        input.seek(index79_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA79_4 = input.LA(1);

                         
                        int index79_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred115_EolParserRules()) ) {s = 15;}

                        else if ( (synpred121_EolParserRules()) ) {s = 16;}

                         
                        input.seek(index79_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA79_5 = input.LA(1);

                         
                        int index79_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred115_EolParserRules()) ) {s = 15;}

                        else if ( (synpred121_EolParserRules()) ) {s = 16;}

                         
                        input.seek(index79_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA79_6 = input.LA(1);

                         
                        int index79_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred115_EolParserRules()) ) {s = 15;}

                        else if ( (synpred121_EolParserRules()) ) {s = 16;}

                         
                        input.seek(index79_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA79_7 = input.LA(1);

                         
                        int index79_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred116_EolParserRules()) ) {s = 17;}

                        else if ( (synpred121_EolParserRules()) ) {s = 16;}

                         
                        input.seek(index79_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA79_9 = input.LA(1);

                         
                        int index79_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred118_EolParserRules()) ) {s = 18;}

                        else if ( (synpred119_EolParserRules()) ) {s = 19;}

                         
                        input.seek(index79_9);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 79, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_operationDeclaration_in_operationDeclarationOrAnnotationBlock259 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_annotationBlock_in_operationDeclarationOrAnnotationBlock263 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_94_in_modelDeclaration282 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_modelDeclaration285 = new BitSet(new long[]{0x0000000000000000L,0x0000000D80000000L});
    public static final BitSet FOLLOW_modelAlias_in_modelDeclaration287 = new BitSet(new long[]{0x0000000000000000L,0x0000000C80000000L});
    public static final BitSet FOLLOW_modelDriver_in_modelDeclaration290 = new BitSet(new long[]{0x0000000000000000L,0x0000000880000000L});
    public static final BitSet FOLLOW_modelDeclarationParameters_in_modelDeclaration293 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_modelDeclaration298 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_96_in_modelAlias313 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_modelAlias316 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_97_in_modelAlias319 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_modelAlias322 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_98_in_modelDriver341 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_modelDriver344 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_99_in_modelDeclarationParameters366 = new BitSet(new long[]{0x0000000000080000L,0x0000001200000000L});
    public static final BitSet FOLLOW_modelDeclarationParameter_in_modelDeclarationParameters369 = new BitSet(new long[]{0x0000000000000000L,0x0000001200000000L});
    public static final BitSet FOLLOW_97_in_modelDeclarationParameters373 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_modelDeclarationParameter_in_modelDeclarationParameters376 = new BitSet(new long[]{0x0000000000000000L,0x0000001200000000L});
    public static final BitSet FOLLOW_100_in_modelDeclarationParameters382 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_modelDeclarationParameter395 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
    public static final BitSet FOLLOW_101_in_modelDeclarationParameter399 = new BitSet(new long[]{0x0000000000004000L});
    public static final BitSet FOLLOW_STRING_in_modelDeclarationParameter402 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_operationDeclaration423 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L});
    public static final BitSet FOLLOW_typeName_in_operationDeclaration433 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_operationDeclaration441 = new BitSet(new long[]{0x0000000000000000L,0x0000010000000000L});
    public static final BitSet FOLLOW_104_in_operationDeclaration445 = new BitSet(new long[]{0x0000000000080000L,0x0000020000000000L});
    public static final BitSet FOLLOW_formalParameterList_in_operationDeclaration448 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_operationDeclaration453 = new BitSet(new long[]{0x0000000000000000L,0x0000040800000000L});
    public static final BitSet FOLLOW_106_in_operationDeclaration457 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L});
    public static final BitSet FOLLOW_typeName_in_operationDeclaration462 = new BitSet(new long[]{0x0000000000000000L,0x0000040800000000L});
    public static final BitSet FOLLOW_statementBlock_in_operationDeclaration468 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_107_in_importStatement488 = new BitSet(new long[]{0x0000000000004000L});
    public static final BitSet FOLLOW_STRING_in_importStatement491 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_importStatement495 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statement_in_block516 = new BitSet(new long[]{0x0000000000085112L,0x34FF010000000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_99_in_statementBlock546 = new BitSet(new long[]{0x0000000000085110L,0x34FF010000000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_block_in_statementBlock549 = new BitSet(new long[]{0x0000000000000000L,0x0000001000000000L});
    public static final BitSet FOLLOW_100_in_statementBlock553 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_formalParameter571 = new BitSet(new long[]{0x0000000000000002L,0x0000040000000000L});
    public static final BitSet FOLLOW_106_in_formalParameter574 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L});
    public static final BitSet FOLLOW_typeName_in_formalParameter578 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_formalParameter_in_formalParameterList612 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_97_in_formalParameterList615 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_formalParameter_in_formalParameterList617 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_108_in_executableAnnotation642 = new BitSet(new long[]{0xFFFFFFFFFFFFFFF0L,0xFFFFFFFFFFFFFFFFL,0x0003FFFFFFFFFFFFL});
    public static final BitSet FOLLOW_logicalExpression_in_executableAnnotation649 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Annotation_in_annotation663 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_executableAnnotation_in_annotation667 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_annotation_in_annotationBlock684 = new BitSet(new long[]{0x0000000000800002L,0x0000100000000000L});
    public static final BitSet FOLLOW_pathName_in_typeName713 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_nativeType_in_typeName717 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_collectionType_in_typeName721 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_pathName735 = new BitSet(new long[]{0x0000000000000000L,0x0000200000000000L});
    public static final BitSet FOLLOW_109_in_pathName737 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_packagedType_in_pathName746 = new BitSet(new long[]{0x0000000000000002L,0x0000400000000000L});
    public static final BitSet FOLLOW_110_in_pathName752 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_pathName757 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_packagedType778 = new BitSet(new long[]{0x0000000000000002L,0x0000800000000000L});
    public static final BitSet FOLLOW_111_in_packagedType781 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_packagedType786 = new BitSet(new long[]{0x0000000000000002L,0x0000800000000000L});
    public static final BitSet FOLLOW_112_in_nativeType815 = new BitSet(new long[]{0x0000000000000000L,0x0000010000000000L});
    public static final BitSet FOLLOW_104_in_nativeType820 = new BitSet(new long[]{0x0000000000004000L});
    public static final BitSet FOLLOW_STRING_in_nativeType823 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_nativeType827 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_collectionType846 = new BitSet(new long[]{0x0000000000000002L,0x0100010000000000L});
    public static final BitSet FOLLOW_104_in_collectionType869 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L});
    public static final BitSet FOLLOW_typeName_in_collectionType874 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_97_in_collectionType879 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L});
    public static final BitSet FOLLOW_typeName_in_collectionType883 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_105_in_collectionType891 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_120_in_collectionType903 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L});
    public static final BitSet FOLLOW_typeName_in_collectionType908 = new BitSet(new long[]{0x0000000000000000L,0x0200000200000000L});
    public static final BitSet FOLLOW_97_in_collectionType913 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L});
    public static final BitSet FOLLOW_typeName_in_collectionType917 = new BitSet(new long[]{0x0000000000000000L,0x0200000200000000L});
    public static final BitSet FOLLOW_121_in_collectionType925 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statementA_in_statement944 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statementB_in_statement948 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_assignmentStatement_in_statementA959 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expressionStatement_in_statementA963 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_forStatement_in_statementA967 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ifStatement_in_statementA973 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_whileStatement_in_statementA977 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_switchStatement_in_statementA981 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_returnStatement_in_statementA985 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_breakStatement_in_statementA989 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_breakAllStatement_in_statementB1001 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_returnStatement_in_statementB1005 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_transactionStatement_in_statementB1009 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_abortStatement_in_statementB1015 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_continueStatement_in_statementB1019 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_throwStatement_in_statementB1023 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_statementB1029 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statement_in_statementOrStatementBlock1040 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statementBlock_in_statementOrStatementBlock1044 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_106_in_expressionOrStatementBlock1053 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_expressionOrStatementBlock1056 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statementBlock_in_expressionOrStatementBlock1060 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_122_in_forStatement1073 = new BitSet(new long[]{0x0000000000000000L,0x0000010000000000L});
    public static final BitSet FOLLOW_104_in_forStatement1076 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_formalParameter_in_forStatement1079 = new BitSet(new long[]{0x0000000000000000L,0x0800000000000000L});
    public static final BitSet FOLLOW_123_in_forStatement1081 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_forStatement1084 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_forStatement1086 = new BitSet(new long[]{0x0000000000085110L,0x34FF050800000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_statementOrStatementBlock_in_forStatement1089 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_124_in_ifStatement1105 = new BitSet(new long[]{0x0000000000000000L,0x0000010000000000L});
    public static final BitSet FOLLOW_104_in_ifStatement1108 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_ifStatement1111 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_ifStatement1113 = new BitSet(new long[]{0x0000000000085110L,0x34FF050800000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_statementOrStatementBlock_in_ifStatement1116 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_elseStatement_in_ifStatement1118 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_125_in_switchStatement1137 = new BitSet(new long[]{0x0000000000000000L,0x0000010000000000L});
    public static final BitSet FOLLOW_104_in_switchStatement1140 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_switchStatement1143 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_switchStatement1145 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_99_in_switchStatement1148 = new BitSet(new long[]{0x0000000000000000L,0xC000001000000000L});
    public static final BitSet FOLLOW_caseStatement_in_switchStatement1151 = new BitSet(new long[]{0x0000000000000000L,0xC000001000000000L});
    public static final BitSet FOLLOW_defaultStatement_in_switchStatement1154 = new BitSet(new long[]{0x0000000000000000L,0x0000001000000000L});
    public static final BitSet FOLLOW_100_in_switchStatement1157 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_caseStatement1176 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_caseStatement1179 = new BitSet(new long[]{0x0000000000000000L,0x0000040000000000L});
    public static final BitSet FOLLOW_106_in_caseStatement1181 = new BitSet(new long[]{0x0000000000085110L,0x34FF050800000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_block_in_caseStatement1185 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statementBlock_in_caseStatement1189 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_127_in_defaultStatement1208 = new BitSet(new long[]{0x0000000000000000L,0x0000040000000000L});
    public static final BitSet FOLLOW_106_in_defaultStatement1211 = new BitSet(new long[]{0x0000000000085110L,0x34FF050800000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_block_in_defaultStatement1215 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statementBlock_in_defaultStatement1219 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_128_in_elseStatement1244 = new BitSet(new long[]{0x0000000000085110L,0x34FF050800000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_statementOrStatementBlock_in_elseStatement1247 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_129_in_whileStatement1260 = new BitSet(new long[]{0x0000000000000000L,0x0000010000000000L});
    public static final BitSet FOLLOW_104_in_whileStatement1263 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_whileStatement1266 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_whileStatement1268 = new BitSet(new long[]{0x0000000000085110L,0x34FF050800000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_statementOrStatementBlock_in_whileStatement1271 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_130_in_returnStatement1293 = new BitSet(new long[]{0x0000000000085110L,0x00FF010080000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_returnStatement1296 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_returnStatement1301 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_131_in_throwStatement1324 = new BitSet(new long[]{0x0000000000085110L,0x00FF010080000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_throwStatement1327 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_throwStatement1332 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_132_in_deleteStatement1355 = new BitSet(new long[]{0x0000000000085110L,0x00FF010080000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_deleteStatement1358 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_deleteStatement1363 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_133_in_breakStatement1389 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_breakStatement1394 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_134_in_breakAllStatement1417 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_breakAllStatement1422 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_135_in_continueStatement1445 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_continueStatement1450 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_136_in_abortStatement1473 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_abortStatement1478 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_137_in_transactionStatement1495 = new BitSet(new long[]{0x0000000000085110L,0x34FF050800000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_NAME_in_transactionStatement1499 = new BitSet(new long[]{0x0000000000085110L,0x34FF050A00000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_97_in_transactionStatement1502 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_transactionStatement1504 = new BitSet(new long[]{0x0000000000085110L,0x34FF050A00000000L,0x0000001C120003FEL});
    public static final BitSet FOLLOW_statementOrStatementBlock_in_transactionStatement1510 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_logicalExpression_in_assignmentStatement1530 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x000000000000FC00L});
    public static final BitSet FOLLOW_138_in_assignmentStatement1536 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_139_in_assignmentStatement1541 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_140_in_assignmentStatement1546 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_141_in_assignmentStatement1551 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_142_in_assignmentStatement1556 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_143_in_assignmentStatement1564 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_assignmentStatement1570 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_assignmentStatement1574 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_postfixExpression_in_expressionStatement1596 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
    public static final BitSet FOLLOW_101_in_expressionStatement1600 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_expressionStatement1603 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_logicalExpression_in_expressionStatement1609 = new BitSet(new long[]{0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_95_in_expressionStatement1614 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_relationalExpression_in_logicalExpression1626 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x00000000000F0000L});
    public static final BitSet FOLLOW_144_in_logicalExpression1632 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_145_in_logicalExpression1637 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_146_in_logicalExpression1642 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_147_in_logicalExpression1647 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_relationalExpression_in_logicalExpression1651 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x00000000000F0000L});
    public static final BitSet FOLLOW_additiveExpression_in_relationalExpression1668 = new BitSet(new long[]{0x0000000000000002L,0x0300002000000000L,0x0000000000F00000L});
    public static final BitSet FOLLOW_148_in_relationalExpression1674 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_relationalExpression_in_relationalExpression1677 = new BitSet(new long[]{0x0000000000000002L,0x0300002000000000L,0x0000000000F00000L});
    public static final BitSet FOLLOW_101_in_relationalExpression1683 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_relationalExpression_in_relationalExpression1686 = new BitSet(new long[]{0x0000000000000002L,0x0300002000000000L,0x0000000000F00000L});
    public static final BitSet FOLLOW_121_in_relationalExpression1716 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_120_in_relationalExpression1721 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_149_in_relationalExpression1726 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_150_in_relationalExpression1731 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_151_in_relationalExpression1736 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_additiveExpression_in_relationalExpression1740 = new BitSet(new long[]{0x0000000000000002L,0x0300002000000000L,0x0000000000F00000L});
    public static final BitSet FOLLOW_multiplicativeExpression_in_additiveExpression1758 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000003000000L});
    public static final BitSet FOLLOW_152_in_additiveExpression1764 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_153_in_additiveExpression1769 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_multiplicativeExpression_in_additiveExpression1773 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000003000000L});
    public static final BitSet FOLLOW_unaryExpression_in_multiplicativeExpression1791 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x000000000C000000L});
    public static final BitSet FOLLOW_154_in_multiplicativeExpression1797 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_155_in_multiplicativeExpression1802 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_unaryExpression_in_multiplicativeExpression1806 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x000000000C000000L});
    public static final BitSet FOLLOW_156_in_unaryExpression1827 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_153_in_unaryExpression1832 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_shortcutOperatorExpression_in_unaryExpression1840 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_postfixExpression_in_shortcutOperatorExpression1852 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000020000000L});
    public static final BitSet FOLLOW_157_in_shortcutOperatorExpression1858 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_itemSelectorExpression_in_postfixExpression1876 = new BitSet(new long[]{0x0000000000000A02L});
    public static final BitSet FOLLOW_set_in_postfixExpression1879 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_featureCall_in_postfixExpression1888 = new BitSet(new long[]{0x0000000000000A02L,0x0000000000000000L,0x0000000040000000L});
    public static final BitSet FOLLOW_158_in_postfixExpression1897 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_postfixExpression1900 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_159_in_postfixExpression1902 = new BitSet(new long[]{0x0000000000000A02L,0x0000000000000000L,0x0000000040000000L});
    public static final BitSet FOLLOW_primitiveExpression_in_itemSelectorExpression1924 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000040000000L});
    public static final BitSet FOLLOW_158_in_itemSelectorExpression1929 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_primitiveExpression_in_itemSelectorExpression1932 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_159_in_itemSelectorExpression1934 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000040000000L});
    public static final BitSet FOLLOW_simpleFeatureCall_in_featureCall1952 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_complexFeatureCall_in_featureCall1956 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_simpleFeatureCall1970 = new BitSet(new long[]{0x0000000000000002L,0x0000010000000000L});
    public static final BitSet FOLLOW_parameterList_in_simpleFeatureCall1973 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_104_in_parameterList1996 = new BitSet(new long[]{0x0000000000085110L,0x00FF030000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_parameterList1999 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_97_in_parameterList2002 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_parameterList2004 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_105_in_parameterList2012 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_complexFeatureCall2040 = new BitSet(new long[]{0x0000000000000000L,0x0000010000000000L});
    public static final BitSet FOLLOW_104_in_complexFeatureCall2045 = new BitSet(new long[]{0x0000000000085110L,0x0001000000000000L,0x0000000300000000L});
    public static final BitSet FOLLOW_lambdaExpression_in_complexFeatureCall2049 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_complexParameterExpression_in_complexFeatureCall2054 = new BitSet(new long[]{0x0000000000000000L,0x0000000200000000L});
    public static final BitSet FOLLOW_97_in_complexFeatureCall2056 = new BitSet(new long[]{0x0000000000085110L,0x0001010000000000L,0x0000000340000000L});
    public static final BitSet FOLLOW_complexParameterExpression_in_complexFeatureCall2060 = new BitSet(new long[]{0x0000000000000000L,0x0000000200000000L});
    public static final BitSet FOLLOW_97_in_complexFeatureCall2062 = new BitSet(new long[]{0x0000000000085110L,0x0001010000000000L,0x0000000340000000L});
    public static final BitSet FOLLOW_lambdaExpressionInBrackets_in_complexFeatureCall2067 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_97_in_complexFeatureCall2074 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001F52000000L});
    public static final BitSet FOLLOW_logicalExpression_in_complexFeatureCall2078 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_lambdaExpressionInBrackets_in_complexFeatureCall2082 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_105_in_complexFeatureCall2089 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_complexParameterExpression2101 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_complexParameterExpression2105 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_nativeType_in_complexParameterExpression2109 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathName_in_complexParameterExpression2113 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_158_in_lambdaExpressionInBrackets2133 = new BitSet(new long[]{0x0000000000080000L,0x0000000000000000L,0x0000000300000000L});
    public static final BitSet FOLLOW_lambdaExpression_in_lambdaExpressionInBrackets2136 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080000000L});
    public static final BitSet FOLLOW_159_in_lambdaExpressionInBrackets2140 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_104_in_lambdaExpressionInBrackets2151 = new BitSet(new long[]{0x0000000000080000L,0x0000000000000000L,0x0000000300000000L});
    public static final BitSet FOLLOW_lambdaExpression_in_lambdaExpressionInBrackets2154 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_lambdaExpressionInBrackets2158 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_formalParameterList_in_lambdaExpression2177 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000300000000L});
    public static final BitSet FOLLOW_160_in_lambdaExpression2183 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_161_in_lambdaExpression2190 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_lambdaExpression2194 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_162_in_newExpression2207 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L});
    public static final BitSet FOLLOW_typeName_in_newExpression2212 = new BitSet(new long[]{0x0000000000000002L,0x0000010000000000L});
    public static final BitSet FOLLOW_parameterList_in_newExpression2216 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_163_in_variableDeclarationExpression2240 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_164_in_variableDeclarationExpression2245 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_variableDeclarationExpression2249 = new BitSet(new long[]{0x0000000000000002L,0x0000040000000000L});
    public static final BitSet FOLLOW_106_in_variableDeclarationExpression2252 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L,0x0000000400000000L});
    public static final BitSet FOLLOW_162_in_variableDeclarationExpression2257 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L,0x0000000400000000L});
    public static final BitSet FOLLOW_typeName_in_variableDeclarationExpression2263 = new BitSet(new long[]{0x0000000000000002L,0x0000010000000000L});
    public static final BitSet FOLLOW_parameterList_in_variableDeclarationExpression2267 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_113_in_literalSequentialCollection2291 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_114_in_literalSequentialCollection2296 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_115_in_literalSequentialCollection2301 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_116_in_literalSequentialCollection2306 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_117_in_literalSequentialCollection2311 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_118_in_literalSequentialCollection2316 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_99_in_literalSequentialCollection2322 = new BitSet(new long[]{0x0000000000085110L,0x00FF011000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_expressionListOrRange_in_literalSequentialCollection2326 = new BitSet(new long[]{0x0000000000000000L,0x0000001000000000L});
    public static final BitSet FOLLOW_100_in_literalSequentialCollection2331 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_logicalExpression_in_expressionRange2346 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_POINT_POINT_in_expressionRange2350 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_expressionRange2353 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_logicalExpression_in_expressionList2374 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_97_in_expressionList2377 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_expressionList2379 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_expressionRange_in_expressionListOrRange2403 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expressionList_in_expressionListOrRange2407 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_119_in_literalMapCollection2426 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_99_in_literalMapCollection2431 = new BitSet(new long[]{0x0000000000085110L,0x00FF011000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_keyvalExpressionList_in_literalMapCollection2434 = new BitSet(new long[]{0x0000000000000000L,0x0000001000000000L});
    public static final BitSet FOLLOW_100_in_literalMapCollection2439 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_keyvalExpression_in_keyvalExpressionList2460 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_97_in_keyvalExpressionList2463 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_keyvalExpression_in_keyvalExpressionList2465 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_additiveExpression_in_keyvalExpression2490 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
    public static final BitSet FOLLOW_101_in_keyvalExpression2494 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_keyvalExpression2497 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literalSequentialCollection_in_primitiveExpression2511 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literalMapCollection_in_primitiveExpression2515 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_primitiveExpression2519 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_featureCall_in_primitiveExpression2523 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathName_in_primitiveExpression2527 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_nativeType_in_primitiveExpression2531 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_collectionType_in_primitiveExpression2537 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_logicalExpressionInBrackets_in_primitiveExpression2541 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_newExpression_in_primitiveExpression2547 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_variableDeclarationExpression_in_primitiveExpression2551 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_104_in_logicalExpressionInBrackets2570 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_logicalExpressionInBrackets2573 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_logicalExpressionInBrackets2577 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_literal0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_annotation_in_synpred16_EolParserRules684 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_104_in_synpred29_EolParserRules869 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L,0x0000000400000000L});
    public static final BitSet FOLLOW_typeName_in_synpred29_EolParserRules874 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_97_in_synpred29_EolParserRules879 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L,0x0000000400000000L});
    public static final BitSet FOLLOW_typeName_in_synpred29_EolParserRules883 = new BitSet(new long[]{0x0000000000000000L,0x0000020200000000L});
    public static final BitSet FOLLOW_105_in_synpred29_EolParserRules891 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_120_in_synpred31_EolParserRules903 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L,0x0000000400000000L});
    public static final BitSet FOLLOW_typeName_in_synpred31_EolParserRules908 = new BitSet(new long[]{0x0000000000000000L,0x0200000200000000L});
    public static final BitSet FOLLOW_97_in_synpred31_EolParserRules913 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L,0x0000000400000000L});
    public static final BitSet FOLLOW_typeName_in_synpred31_EolParserRules917 = new BitSet(new long[]{0x0000000000000000L,0x0200000200000000L});
    public static final BitSet FOLLOW_121_in_synpred31_EolParserRules925 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statementA_in_synpred32_EolParserRules944 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_assignmentStatement_in_synpred33_EolParserRules959 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expressionStatement_in_synpred34_EolParserRules963 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_elseStatement_in_synpred48_EolParserRules1118 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_synpred57_EolParserRules1499 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_97_in_synpred57_EolParserRules1502 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_NAME_in_synpred57_EolParserRules1504 = new BitSet(new long[]{0x0000000000000002L,0x0000000200000000L});
    public static final BitSet FOLLOW_postfixExpression_in_synpred63_EolParserRules1596 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
    public static final BitSet FOLLOW_101_in_synpred63_EolParserRules1600 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_logicalExpression_in_synpred63_EolParserRules1603 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_148_in_synpred74_EolParserRules1674 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_relationalExpression_in_synpred74_EolParserRules1677 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_101_in_synpred74_EolParserRules1683 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_relationalExpression_in_synpred74_EolParserRules1686 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred74_EolParserRules1713 = new BitSet(new long[]{0x0000000000085110L,0x00FF010000000000L,0x0000001C12000000L});
    public static final BitSet FOLLOW_additiveExpression_in_synpred74_EolParserRules1740 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NAME_in_synpred94_EolParserRules2101 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_106_in_synpred104_EolParserRules2252 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L,0x0000000400000000L});
    public static final BitSet FOLLOW_162_in_synpred104_EolParserRules2257 = new BitSet(new long[]{0x0000000000080000L,0x00FF000000000000L,0x0000000400000000L});
    public static final BitSet FOLLOW_typeName_in_synpred104_EolParserRules2263 = new BitSet(new long[]{0x0000000000000002L,0x0000010000000000L});
    public static final BitSet FOLLOW_parameterList_in_synpred104_EolParserRules2267 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expressionRange_in_synpred112_EolParserRules2403 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literalSequentialCollection_in_synpred115_EolParserRules2511 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literalMapCollection_in_synpred116_EolParserRules2515 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_featureCall_in_synpred118_EolParserRules2523 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathName_in_synpred119_EolParserRules2527 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_collectionType_in_synpred121_EolParserRules2537 = new BitSet(new long[]{0x0000000000000002L});

}
